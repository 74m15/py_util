#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 11 14:41:53 2017

@author: pasquale
"""
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.mime.application import MIMEApplication

from .common import services, security_decode

import atexit
import logging
import os
import smtplib

# TODO: fix this global singleton variable

EMAIL_SERVICE = "service.email"

class EmailUtil(object):
    
    def __init__(self, use_mail, config, logger=None):
        self._use_mail = use_mail
        self._smtp = None
        self.logger = logger or logging.getLogger(config.logger)
        
        try:
            if (self._use_mail):
                self._smtp = smtplib.SMTP(config.smtp.server, config.smtp.port)
                
                if (config.smtp.tls):
                    self._smtp.starttls()
                
                self._smtp.ehlo()
                self._smtp.esmtp_features["auth"] = "LOGIN PLAIN"
                self._smtp.login(
                    security_decode(str(config.smtp.username)), 
                    security_decode(str(config.smtp.password)))
                
                atexit.register(self._destroy_me)
            else:
                self.logger.warning("Skipping SMTP connection")
            
            services[EMAIL_SERVICE] = self
        except Exception as ex:
            self.logger.debug("Cannot open SMTP client connection")
            
            raise Exception("Cannot connect to SMTP server", ex)
    
    
    def send_mail(self, mail, template, context=None):
        from_addr = mail["from"] # "from" is a reserved keyword
        to_addr = mail.to
        cc_addr = mail.cc
        mime_type = "plain" if (not mail.mime) else mail.mime
        subject = mail.subject
                
        # TODO: some code to get JINJA2 template instance from pool
        body = template.render(**(dict() if context is None else context))

        msg = MIMEMultipart()
        msg['From'] = from_addr
        msg['To'] = to_addr
        msg['Subject'] = subject
        msg.attach(MIMEText(body, mime_type))
        
        recipients = [s.strip() for s in to_addr.split(",")]
        
        if (cc_addr):
            msg['Cc'] = cc_addr
            for s in cc_addr.split(","):
                recipients.append(s.strip())
        
        if (mail.attach and self._use_mail):
            attachment_list = mail.attach

            print(mail.attach, mail.attach[0], mail.attach[1])
            
            if (isinstance(attachment_list, str) or isinstance(attachment_list, unicode)):
                attachment_list = [ attachment_list ]
            
            for attachment in attachment_list:
                with open(attachment, "rb") as f:
                    obj = MIMEApplication(f.read())
                    obj.add_header("Content-Disposition", "attachment", filename="{0}".format(os.path.basename(attachment)))
                
                msg.attach(obj)
        
        if (self._use_mail):
            self._smtp.sendmail(from_addr, recipients, msg.as_string())
        else:
            self.logger.warning("Cannot send mail ('use_mail' is False)")
    
    
    def _destroy_me(self):
        try:
            if (self._smtp):
                self._smtp.quit()
        except:
            # silently ignore exception
            pass


