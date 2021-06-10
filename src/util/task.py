# -*- coding: utf-8 -*-
"""
Created on Wed Jul 11 18:53:22 2018

@author: CAPUANO-P
"""

import cmd
import schedule
import subprocess
import threading
import time

from telegram.ext import Updater, CommandHandler
from util.common import Wrap
from util.security import security_decode

TASK_SUBPROCESS = "subprocess"

class TaskDef(object):
        
    def __init__(self, name, type, logger, description=None, conflict=None, args=None):
        self._name = name
        self._type = type
        self._description = description
        self._conflict = conflict
        self._args = args
        self._logger = logger
        self._command = None
        self._schedule = dict()
        self._runnable = True
        self._singleton = True
    
    
    @property
    def name(self):
        return self._name
    
    @property
    def type(self):
        return self._type
    
    @property
    def command(self):
        return self._command
    
    @command.setter
    def command(self, value):
        self._command = value
    
    @property
    def singleton(self):
        return self._singleton
    
    @singleton.setter
    def singleton(self, value):
        self._singleton = bool(value)
    
    @property
    def runnable(self):
        return self._runnable
    
    @runnable.setter
    def runnable(self, value):
        self._runnable = bool(value)
    
    def has_schedule(self):
        return len(self._schedule) > 0
    
    def add_schedule(self, task_schedule):
        
        def get_key(schedule_):
            return f"every {schedule_.interval} {schedule_.unit} at {schedule_.at_time}"
        
        key = get_key(task_schedule)
        
        self._schedule[key] = task_schedule
    
    def do_schedule(self):
        self._logger.debug(f"Scheduling task {self.name}")
        
        for key, task_schedule in self._schedule.items():
            self._logger.debug()
            task_schedule.do(self)
    
    def __call__(self, *args, **kwargs):
        self._logger.info("Running task \"{0}\"".format(self.name))
        
        if self.type == TASK_SUBPROCESS:
            self._logger.debug("Invoking command: {0}".format(self.command))
            
            self.command.bind_context(kwargs)
            
            subprocess.call(self.command)

            self._logger.info("Task \"{0} invoked".format(self.name))

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
        
        name = arg.strip()
        
        task = self._manager.get_task(name)
        
        if task is not None:
            self._logger.debug(f"Running command {name}")
            task()
        else:
            self._logger.error(f"No task found with name '{name}'")
        
        return False
    
    def do_tasklist(self, arg):
        "Print list of tasks."
        
        for task_name in sorted(self._manager.task_list.keys()):
            print(f"\t{task_name}")
        
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
        
        if self._manager.has_scheduler:
            self._manager.scheduler.stop()
        
        if self._manager.has_telegram:
            self._manager.telegram.stop()
        
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
    
    def __init__(self, token, manager, logger):
        self._manager = manager
        self._logger = logger
        self._running = False
        self._updater = Updater(token, use_context=True)
        self._dispatcher = self._updater.dispatcher
        
        self._dispatcher.add_handler(CommandHandler("test", self.do_test))
        self._dispatcher.add_handler(CommandHandler("run", self.do_run))
        self._dispatcher.add_handler(CommandHandler("tasklist", self.do_tasklist))
        
        self._logger.info("TaskTelegramController initialized")
    
    @property
    def running(self):
        return self._running
    
    def do_test(self, update, context):
        print(">>> update <<<\n", update)
        print(">>> context <<<\n", context.args)
    
    def do_run(self, update, context):
        "Run a task by name."
        
        if len(context.args) > 0:
            name = context.args[0]
        
            task = self._manager.get_task(name)
            
            if task is not None:
                message = f"Running command '{name}'"
                
                self._logger.debug(message)
                kwargs = Wrap.prepare_context(context.args)
                task(**kwargs)
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
        self._init_task_list(config.tasklist)
        self._init_shell(bool(config.shell))
        self._init_scheduler(bool(config.scheduler))
        self._init_telegram(config.telegram)

        self._logger.info("TaskManager initialized")
    
    
    def _init_task_list(self, task_list):
        for task in task_list:
            self._logger.debug("Preparing task: {0}".format(task))

            task_def = TaskDef(task.name, task.type, self._logger, task.description, task.conflict, task.args)
            
            task.singleton = task.singleton
            
            if task.type == TASK_SUBPROCESS:
                task_def.command = task.command
            
            if bool(task.schedule):
                for task_schedule in task.schedule:
                    interval = task_schedule[0]
                    unit = task_schedule[1]
    
                    try:
                        job = getattr(schedule.every(interval), unit)
    
                        self._logger.debug(
                            "Task \"{0}\": defined schedule for every {1} {2}".format(task.name, interval, unit))
    
                        if len(task_schedule) > 2:
                            at_time = task_schedule[2]
                            job = job.at(at_time)
    
                            self._logger.debug("Task \"{0}\": added run time at {1}".format(task.name, at_time))
                        
                        job.do(task_def)
                        
                        task_def.add_schedule(job)
                    except:
                        self._logger.error('ERROR: unit [{}] not valid!'.format(unit))
            
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
            
