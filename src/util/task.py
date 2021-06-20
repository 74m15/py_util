# -*- coding: utf-8 -*-
"""
Created on Wed Jul 11 18:53:22 2018

@author: CAPUANO-P
"""

import cmd
import concurrent.futures as cf
import schedule
import subprocess
import threading
import time

from collections import defaultdict, ChainMap
from functools import partial
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CallbackQueryHandler, CommandHandler, ConversationHandler, MessageHandler, Filters
from util.common import Wrap
from util.security import security_decode

TASK_TYPE_SUBPROCESS = "subprocess"

TASK_RUN_SINGLETON = -1
TASK_RUN_CONFLICT = -2

class Task(object):
        
    def __init__(self, name, type, logger, options):
        self._name = name
        self._type = type
        self._logger = logger
        
        self._description = options.get("description")
        self._singleton = options.get("singleton")
        self._conflict = options.get("conflict")
        self._args = options.get("args")
        self._command = options.get("command")
        
#        self._schedule = dict()
        self._runnable = True
        
        self._init_default_args()
    
    
    def _init_default_args(self):
        self._default_args = dict()
        
        if self._args is None:
            return
        
        try:
            for name in self._args:
                arg = self._args[name]
                
                if arg.default is not None:
                    sep_char = "\"" if arg.type == "str" else ""
                    
                    self._default_args[name] = eval(f"{arg.type}({sep_char}{arg.default}{sep_char})")
        except Exception as ex:
            self._logger.warning(f"Cannot set default args for task '{self._name}': {ex}")
    
    @property
    def name(self):
        return self._name
    
    @property
    def type(self):
        return self._type
    
    @property
    def description(self):
        return self._description
    
    @property
    def conflict(self):
        return self._conflict
    
    @property
    def args(self):
        return self._args
    
    @property
    def default_args(self):
        return self._default_args
    
    @property
    def command(self):
        return self._command
    
    @property
    def singleton(self):
        return self._singleton
    
    @property
    def runnable(self):
        return self._runnable
    
    @runnable.setter
    def runnable(self, value):
        self._runnable = bool(value)
    
#    def has_schedule(self):
#        return len(self._schedule) > 0
#    
#    def add_schedule(self, task_schedule):
#        
#        def get_key(schedule_):
#            return f"every {schedule_.interval} {schedule_.unit} at {schedule_.at_time}"
#        
#        key = get_key(task_schedule)
#        
#        self._schedule[key] = task_schedule
#    
#    def do_schedule(self):
#        self._logger.debug(f"Scheduling task {self.name}")
#        
#        for key, task_schedule in self._schedule.items():
#            self._logger.debug()
#            task_schedule.do(self)
    
    def __call__(self, *args, **kwargs):
        self._logger.info(f"Running task '{self.name}'")
        
        if self.type == TASK_TYPE_SUBPROCESS:
            self._logger.debug(f"Invoking command: {self.command}")
            
            self.command.bind_context(ChainMap(kwargs, self._default_args))
            
            subprocess.call(self.command)

            self._logger.info(f"Task '{self.name}' invoked")

class TaskShell(cmd.Cmd):
    intro = "TaskManager runtime shell. Type 'help' or '?' for available commands.\n"
    prompt = "> "
    file = None
    
    def __init__(self, manager, logger):
        super(TaskShell, self).__init__()
        self._manager = manager
        self._logger = logger
    
    def emptyline(self):
        return False
        
    def do_run(self, arg):
        "Run a task by name."
        
        args = [s.strip() for s in arg.split(" ")]
        
        name = args[0]
        kwargs = Wrap.prepare_context(args[1:])
        task = self._manager.get_task(name)
        
        if task is not None:
            self._logger.debug(f"Running command {name}")
            result = self._manager.run_task(task, kwargs)
        else:
            self._logger.error(f"No task found with name '{name}'")
        
        return False
    
    def do_tasklist(self, arg):
        "Print list of tasks."
        
        for task_name in sorted(self._manager.task_list.keys()):
            print(f"\t{task_name}")
        
        return False
    
    def do_task_status(self, arg):
        "Print task execution status."
        
        self._manager.task_status()
        
        return False
    
    def do_scheduler_start(self, arg):
        "Start TaskScheduler."
        
        self._manager.scheduler.start()
        
        return False
    
    def do_scheduler_stop(self, arg):
        "Stop TaskScheduler."
        
        self._manager.scheduler.stop()
        
        return False
    
    def do_scheduler_status(self, arg):
        "Check TaskScheduler status."
        
        print(f"TaskScheduler is running: {self._manager.scheduler.running}")
        
        return False
    
    def do_telegram_start(self, arg):
        "Start TaskTelegramController."
        
        self._manager.telegram.start()
        
        return False
    
    def do_telegram_stop(self, arg):
        "Stop TaskTelegramController."
        
        self._manager.telegram.stop()
        
        return False
    
    def do_telegram_status(self, arg):
        "Check TaskTelegramController status."
        
        print(f"TaskTelegramController is running: {self._manager.telegram.running}")
        
        return False
    
    def do_shutdown(self, arg):
        "Stop the shell and all the running services."
        
        print("Stopping services...")
        
        if self._manager.has_scheduler:
            self._manager.scheduler.stop()
            print("Scheduler stopped")
        
        if self._manager.has_telegram:
            self._manager.telegram.stop()
            print("Telegram stopped")
        
        print("TaskManager will stop after all running tasks complete.")
        
        return True
    
    def do_exit(self, arg):
        "Stop the shell."
        
        return True

class TaskScheduler(object):
    
    def __init__(self, manager, logger):
        self._manager = manager
        self._logger = logger
        self._running = False
        self._scheduler = None
        
        self._logger.info("TaskScheduler initialized")
    
    @property
    def running(self):
        return self._running
    
    def __run(self):
        while self._running:
            schedule.run_pending()
            
            time.sleep(0.25)
    
    def start(self):
        if self._running:
            self._logger.warning("TaskScheduler is already running.")
        else:
            self._running = True
            self._manager.has_scheduler = True
            
            self._scheduler = threading.Thread(name="TaskScheduler", target=self.__run)
            self._scheduler.start()
            
            self._logger.info("TaskScheduler is running.")
    
    def stop(self):
        if self._running:
            self._running = False
            
            self._scheduler.join()
            self._scheduler = None
            
            self._logger.info("TaskScheduler stopped.")
        else:
            self._logger.info("TaskScheduler not running.")

class TaskTelegramController(object):
    
    s_ASK_TASK, s_ASK_ARGS, s_ASK_CONFIRM, s_RESULT = range(4)
    UD_RUN = "run"
    CB_RUN = "__run__"
    
    def __init__(self, token, manager, logger):
        self._manager = manager
        self._logger = logger
        self._running = False
        self._updater = Updater(token, use_context=True)
        self._dispatcher = self._updater.dispatcher
        
        run_handler = ConversationHandler(
            entry_points = [ CommandHandler("run", self.do_run_start) ],
            states = {
                TaskTelegramController.s_ASK_TASK: [
                    CallbackQueryHandler(self.do_run_ask_task)
                ],
                
                TaskTelegramController.s_ASK_ARGS: [
                    CallbackQueryHandler(self.do_run_ask_args),
                    MessageHandler(Filters.text, self.do_run_ask_args),
                ],
                
                TaskTelegramController.s_ASK_CONFIRM: [
                    CallbackQueryHandler(self.do_run_ask_confirm, pattern="^yes$")
                ],
                
                TaskTelegramController.s_RESULT: [
                    CallbackQueryHandler(self.do_run_confirm)
                ]
            },
                    
            fallbacks = [ CommandHandler("cancel", self.do_run_cancel) ]
        )
    
        self._dispatcher.add_handler(run_handler)
        self._dispatcher.add_handler(CommandHandler("tasklist", self.do_tasklist))
        
        self._logger.info("TaskTelegramController initialized")
    
    @property
    def running(self):
        return self._running
    
    def do_run_start(self, update, context):
        context.user_data[TaskTelegramController.UD_RUN] = dict()
        
        if len(context.args) > 0:
            name = context.args[0]
        
            self._logger.debug(f"/run command received task name '{name}'")
            
            task = self._manager.get_task(name)
            
            if task is not None:
                self._logger.debug(f"/run command preparing to run task '{name}'")
                
                context.user_data[TaskTelegramController.UD_RUN]["task"] = task
                
                if task.args is None:
                    next = TaskTelegramController.s_ASK_CONFIRM
                    
                    context.user_data[TaskTelegramController.UD_RUN]["args"] = dict()
                    
                    self._prepare_do_run_ask_confirm(update, context)
                else:
                    next = TaskTelegramController.s_ASK_ARGS
                    
                    args = dict()
                    context.user_data[TaskTelegramController.UD_RUN]["args"] = args
                    
                    for k,v in task.default_args.items():
                        args[k] = v
                    
                    self._prepare_do_run_ask_args(update, context)
            else:
                self._logger.debug(f"Task name '{name}' not found in task list")
                
                next = TaskTelegramController.s_ASK_TASK
                
                self._prepare_do_run_ask_task(f"Task '{name}' not found.", update, context)
        else:
            self._logger.debug(f"/run command received no command: need to ask task")
            
            next = TaskTelegramController.s_ASK_TASK
                
            self._prepare_do_run_ask_task("", update, context)
        
        return next
    
    def  _prepare_do_run_ask_task(self, prefix, update, context):
        message = f"{prefix} Choose one of the available tasks".strip()
        
        keyboard = InlineKeyboardMarkup(
                [[InlineKeyboardButton(text=name, callback_data=name) for name in sorted(self._manager.task_list.keys())]])
        
        update.message.reply_text(text=message, reply_markup=keyboard)
    
    def do_run_ask_task(self, update, context):
        update.callback_query.answer()
        update.callback_query.message.edit_reply_markup()
        update.callback_query.message.reply_text(text=f"You asked to run task '{update.callback_query.data}'")
        
        # TO DO: implementare logica di selezione task e instradamento su gestione argomenti
    
    def  _prepare_do_run_ask_args(self, update, context):
        task = context.user_data[TaskTelegramController.UD_RUN]["task"]
        args = context.user_data[TaskTelegramController.UD_RUN]["args"]
        
        keys = list()
        
        for arg_name in task.args:
            if arg_name in args:
                arg_text = f"{arg_name} ({args[arg_name]})"
            else:
                arg_text = f"{arg_name}"
            
            key = InlineKeyboardButton(text=arg_text, callback_data=arg_name)
        
            keys.append([key])
        
        keys.append([InlineKeyboardButton(text="Run task", callback_data=TaskTelegramController.CB_RUN)])
        
        keyboard = InlineKeyboardMarkup(keys)
        
        update.message.reply_text(text="Choose one of the expected arguments", reply_markup=keyboard)
    
    def do_run_ask_args(self, update, context):
        task = context.user_data[TaskTelegramController.UD_RUN]["task"]
        args = context.user_data[TaskTelegramController.UD_RUN]["args"]
        
        if update.callback_query is not None:
            update.callback_query.answer()
            update.callback_query.message.edit_reply_markup()
            
            if update.callback_query.data == TaskTelegramController.CB_RUN:
                self._prepare_do_run_ask_confirm(update, context)
            
                return TaskTelegramController.s_ASK_CONFIRM
            else:
                arg = update.callback_query.data
                value = args.get(arg)
                
                if value is None:
                    message = \
                        f"Argument *{arg}*: _{task.args[arg].description}_\n" \
                        f"Insert a value \\(expected type: *{task.args[arg].type}*\\)"
                else:
                    delimiter = "\"" if isinstance(value, str) else ""
                    message = \
                        f"Argument '*{arg}*': _{task.args[arg].description}_\n" \
                        f"Insert a value \\(expected type: *{task.args[arg].type}*\\): " \
                        f"current value is `{value}`"
                
                context.user_data[TaskTelegramController.UD_RUN]["arg"] = arg
                
                update.callback_query.message.reply_markdown_v2(text=message)
        else:
            arg = context.user_data[TaskTelegramController.UD_RUN]["arg"]
            value = update.message.text
            args[arg] = value
            
            del context.user_data[TaskTelegramController.UD_RUN]["arg"]
            
            self._prepare_do_run_ask_args(update, context)
    
    def  _prepare_do_run_ask_confirm(self, update, context):
        task = context.user_data[TaskTelegramController.UD_RUN]["task"]
        args = context.user_data[TaskTelegramController.UD_RUN]["args"]
        
        if len(args) > 0:
            message = f"Do you confirm to run task *{task.name}* with the following arguments?"
            
            for k, v in args.items():
                message += f"\n_{k}_ \\= `{v}`"
        else:
            message = f"Do you confirm to run task *{task.name}* with no arguments?"
            
        keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton(text="Yes", callback_data="yes"), 
                 InlineKeyboardButton(text="No", callback_data="no")]])
        
        if update.callback_query is None:
            update.message.reply_markdown_v2(text=message, reply_markup=keyboard)
        else:
            update.callback_query.message.reply_markdown_v2(text=message, reply_markup=keyboard)
        
        return TaskTelegramController.s_ASK_CONFIRM
    
    def do_run_ask_confirm(self, update, context):
        task = context.user_data[TaskTelegramController.UD_RUN]["task"]
        args = context.user_data[TaskTelegramController.UD_RUN]["args"]
        
        result = self._manager.run_task(task, args)
                
        if isinstance(result, int):
            if result == TASK_RUN_SINGLETON:
                message = f"Task '{task.name}' is singleton and is running just now: cannot run again"
            elif result == TASK_RUN_CONFLICT:
                message = f"Task '{task.name}' cannot be run because another conflicting task is already running"
            else:
                message = f"Cannot run task '{task.name}' just now"
        else:
            message = f"Running command '{task.name}'"
        
        update.callback_query.answer()
        update.callback_query.message.edit_reply_markup()
        
        update.callback_query.message.reply_text(text=message)
        
        del context.user_data[TaskTelegramController.UD_RUN]
        
        return ConversationHandler.END
        
    
    def do_run_confirm(self, update, context):
        pass 
    
    def do_run_cancel(self, update, context):
        
        del context.user_data[TaskTelegramController.UD_RUN]
        
        update.message.reply_text(text="No command will be run.")
        
        return ConversationHandler.END
    
    def do_run(self, update, context):
        "Run a task by name."
        
        if len(context.args) > 0:
            name = context.args[0]
        
            task = self._manager.get_task(name)
            
            if task is not None:
                message = f"Running command '{name}'"
                
                self._logger.debug(message)
                kwargs = Wrap.prepare_context(context.args)
                
                result = self._manager.run_task(task, kwargs)
                
                if isinstance(result, int):
                    if result == TASK_RUN_SINGLETON:
                        message = f"Task '{task.name}' is singleton and is running just now: cannot run again"
                    elif result == TASK_RUN_CONFLICT:
                        message = f"Task '{task.name}' cannot be run because another conflicting task is already running"
                    else:
                        message = f"Cannot run task '{task.name}' just now"
            else:
                message = f"No task found with name '{name}'"
                
                self._logger.error(message)
        else:
            message = "Expected syntax: /run taskname <args>"
        
        update.message.reply_text(text=message)
    
    def do_tasklist(self, update, context):
        "Print list of tasks."
        
        tasklist = sorted(self._manager.task_list.keys())
        
        if len(tasklist) > 0:
            message = f"Tasklist: {', '.join(tasklist)}"
        else:
            message = "Empty tasklist!"
        
        update.message.reply_text(text=message)
    
    def start(self):
        if self._running:
            self._logger.warning("TaskTelegramController is already running.")
        else:
            self._running = True
            self._manager.has_telegram_controller = True
            
            self._updater.start_polling()
            
            self._logger.info("TaskTelegramController is running.")
    
    def stop(self):
        if self._running:
            self._running = False
            
            self._updater.stop()
            
            self._logger.info("TaskTelegramController stopped.")
        else:
            self._logger.info("TaskTelegramController not running.")

class TaskManager(object):

    def __init__(self, config, context, logger):
        self._config = config
        self._context = context
        self._logger = logger

        self._task_list = dict()
        
        self._init_monitor()
        self._init_task_list(config.tasklist)
        self._init_shell(bool(config.shell))
        self._init_scheduler(bool(config.scheduler))
        self._init_telegram(config.telegram)
        
        self._logger.info("TaskManager initialized")
    
    
    def _init_monitor(self):
        self._pool = cf.ThreadPoolExecutor(thread_name_prefix="TaskManager")
        self._running_task = defaultdict(int)
        self._futures = dict()
    
    
    def _init_task_list(self, task_list):
        for task in task_list:
            self._logger.debug(f"Preparing task: {task}")

            task_def = Task(task.name, task.type, self._logger, task.to_dict())
                        
            if bool(task.schedule):
                for task_schedule in task.schedule:
                    interval = task_schedule[0]
                    unit = task_schedule[1]
    
                    try:
                        job = getattr(schedule.every(interval), unit)
    
                        self._logger.debug(
                            f"Task '{task.name}': defined schedule for every {interval} {unit}")
    
                        if len(task_schedule) > 2:
                            at_time = task_schedule[2]
                            job = job.at(at_time)
    
                            self._logger.debug(f"Task '{task.name}': added run time at {at_time}")
                        
                        job.do(self.get_runner(task_def))
                        
#                        task_def.add_schedule(job)
                    except:
                        self._logger.error(f"ERROR: unit [{unit}] not valid!")
            
            self._task_list[task.name] = task_def
    
    def _init_shell(self, has_shell):
        if self._context.get("shell") is not None and self._context["shell"]:
            self._logger.debug("Shell requested: activating")
            
            has_shell = True
        
        if has_shell:
            self._shell = TaskShell(self, self._logger)
        else:
            self._shell = None
    
    @property
    def shell(self):
        return self._shell
    
    @property
    def has_shell(self):
        return self._shell is not None
    
    def _init_scheduler(self, has_scheduler):
        self._scheduler = TaskScheduler(self, self._logger)
        
        if self._context.get("scheduler") is not None and self._context["scheduler"]:
            self._logger.debug("Scheduler requested: activating")
            
            has_scheduler = True
        
        self._has_scheduler = has_scheduler
    
    @property
    def scheduler(self):
        return self._scheduler
    
    @property
    def has_scheduler(self):
        return self._has_scheduler
    
    @has_scheduler.setter
    def has_scheduler(self, value):
        self._has_scheduler = bool(value)
    
    def _init_telegram(self, telegram):
        token = security_decode(telegram.token)
        has_telegram = telegram.started
        
        self._telegram = TaskTelegramController(token, self, self._logger)
        
        if self._context.get("telegram") is not None and self._context["telegram"]:
            self._logger.debug("Telegram Controller requested: activating")
            
            has_telegram = True
        
        self._has_telegram = has_telegram
    
    @property
    def telegram(self):
        return self._telegram
    
    @property
    def has_telegram(self):
        return self._has_telegram
    
    @has_telegram.setter
    def has_telegram(self, value):
        self._has_telegram = bool(value)
    
    @property
    def task_list(self):
        return self._task_list
    
    def get_task(self, name):
        return self._task_list.get(name)
    
    def refresh_status(self):
        for name in list(self._running_task.keys()):
            if self._running_task[name] == 0:
                del self._running_task[name]
    
    def get_runner(self, task):
        
        def composite(task, **kwargs):
            self._running_task[task.name] += 1

            try:            
                return task(**kwargs)
            except Exception as ex:
                self._logger.warning(f"Task '{task.name} raised exception {type(ex)}: {ex}")
                
                raise ex
            finally:
                self._running_task[task.name] -= 1
                
                if self._running_task[task.name] == 0:
                    del self._running_task[task.name]
        
        def runner(task, *args, **kwargs):
            if task.singleton and self._running_task[task.name] > 0:
                self._logger.warning(f"Task '{task.name}' is singleton and is running just now: cannot run again")
            
                return TASK_RUN_SINGLETON
            
            if task.conflict is not None and len(task.conflict) > 0:
                if any([bool(self._running_task[name] > 0) for name in task.conflict]):
                    self._logger.warning(f"Task '{task.name}' cannot be run because another conflicting task is already running")
                    
                    return TASK_RUN_CONFLICT
            
            future = self._pool.submit(composite, task=task, **kwargs)
            
            return future
        
        return partial(runner, task)
    
    def run_task(self, task, kwargs):
        return self.get_runner(task)(**kwargs)
    
    def task_status(self):
        self.refresh_status()
        
        try:
            l_n = max(10, *[len(n) for n in self._running_task.keys()])
            
            print(f"\nTask{' '*(l_n - 4)}|Running")
            print(f"{'-' * l_n}+-------")
            
            for k,v in self._running_task.items():
                print(f"{k}{' '*(l_n - len(k))}|{v:>7}")
            
            print()
        except:
            print("\nNo running task.\n")
    
    def execute(self):
        self._logger.info("TaskManager running...")
        
        if self.has_scheduler:
            self.scheduler.start()
        
        if self.has_telegram:
            self.telegram.start()
        
        if self.has_shell:
            try:
                self.shell.cmdloop()
            except KeyboardInterrupt:
                self._logger.info("Terminating TaskManager...")
                
                if self.has_scheduler:
                    self.scheduler.stop()
                
                if self.has_telegram:
                    self.telegram.stop()
            except Exception as ex:
                self._logger.error(f"Unexpected shell error: {ex}")
            
