#!/usr/bin/python3

# sudo pip3 install mysql-connector==2.1.6
# sudo pip3 install pysftp

import logging
import logging.handlers
import pysftp
import os
import configparser
import sys
import inspect
import mysql.connector
import shutil
import re
import sqlite3
from datetime import datetime

##############################################################################
## settings class
##############################################################################
class settings:
    inifile = ''
    config = False

    # ---------------------------------------------------------------------------------------------------------
    # Init class
    def __init__(self,inifile=''):
        self.inifile = inifile      # filename
        self.readSettings()

    # ---------------------------------------------------------------------------------------------------------
    # Read/process inifile
    def readSettings(self):
        if not os.path.isfile(self.inifile):
            return 'Missing configuration file!'
        # Read .ini file
        self.config = configparser.RawConfigParser()
        self.config.read(self.inifile)
        return ''

    # ---------------------------------------------------------------------------------------------------------
    # Get all sections
    def getSections(self):
        return self.config.sections()

    # ---------------------------------------------------------------------------------------------------------
    # Return True if section exists
    def sectionExists(self,section):
        return self.config.has_section(section)

    # ---------------------------------------------------------------------------------------------------------
    # Read value as string, return default if not found
    # [section]
    # key = value
    def getString(self,section,key,default=''):
            if self.config.has_option(section,key):
                return self.config.get(section,key)
            else:
                return default

    # ---------------------------------------------------------------------------------------------------------
    # Read value as integer, return default if not found
    # [section]
    # key = value
    def getInt(self,section,key,default=0):
            if self.config.has_option(section,key):
                return self.config.getint(section,key)
            else:
                return default

##############################################################################
## transfer class
## This is a "in-work" version and currently only sftp is supported
##############################################################################
class transfer:
    logclass = False
    conn = False
    conntype = 'sftp'
    # remote connection parameters
    host = ''
    port = 22
    username = ''
    password = ''
    privkey = ''
    #
    basepath = ''
    #
    pwd = ''
    sid = ''
    ts = ''

    # ---------------------------------------------------------------------------------------------------------
    # Init class
    def __init__(self,sid,logclass,basepath):
        self.sid = sid
        self.logclass = logclass
        self.pwd = ''
        self.basepath = basepath
        self.updateTimeStamp()

    # ---------------------------------------------------------------------------------------------------------
    # Set timestamp 
    def updateTimeStamp(self, ts=False):
        if ts == False:
            self.ts = datetime.now()
        else:
            self.ts = ts            

    # ---------------------------------------------------------------------------------------------------------
    # Store remote settings
    def setRemote(self,host='',port=22,user='',privkey='',password='',conntype='sftp'):
        self.host=host
        self.port=port
        self.username=user
        self.password=password
        self.privkey=privkey
        self.conntype=conntype

    # ---------------------------------------------------------------------------------------------------------
    # Some directories have additional macros
    def __chgPath(self,path):
        # {date#1} => YYYYMMDD
        return path.replace('{date#1}',self.ts.strftime('%Y%m%d'))

    # ---------------------------------------------------------------------------------------------------------
    # Connect to remote sftp
    def connect(self):
        self.logclass.logInfo('Connect to {host} using {conntype}'.format(host=self.host,conntype=self.conntype))
        if self.conntype == 'sftp':

            # TODO: For now we disable hostkey checking, should be on ...
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None

            if self.privkey == '':
                # Auth with user/password
                try:
                    self.logclass.logInfo('Auth with password')
                    self.conn = pysftp.Connection(self.host,username=self.username,password=self.password,cnopts=cnopts)
                except Exception as e:
                    self.logclass.logError('Failed to connect',self.lineno())
                    self.conn = False
                    return False
            else:
                # Auth with user/privkey
                try:
                    if self.privkey[0] != '/':
                        ppk = os.path.join(self.basepath, self.privkey)
                    else:
                        ppk = self.privkey
                    # self.logclass.logInfo('Using private key {f}'.format(f=ppk))
                    self.logclass.logInfo('Auth with private key')
                    self.conn = pysftp.Connection(self.host,username=self.username,private_key=ppk,cnopts=cnopts)
                except Exception as e:
                    self.logclass.logError('Failed to connect',self.lineno())
                    self.conn = False
                    return False

            # Save current remote path
            self.pwd = self.conn.pwd

            # Success
            return True
        else:
            # Not supported
            self.logclass.logError('Unknown conntype',self.lineno())
            return False

    # ---------------------------------------------------------------------------------------------------------
    # Disconnect sftp
    def disconnect(self):
        if not self.conn == False:
            if self.conntype == 'sftp':
                # We have a connection; disconnect
                self.conn.close()
                self.logclass.logInfo('Remote disconnected')

    # ---------------------------------------------------------------------------------------------------------
    # Get current line in code when called
    def lineno(self):
        return inspect.currentframe().f_back.f_lineno

    # ---------------------------------------------------------------------------------------------------------
    # Joining path for remote sftp
    def joinpath(self,p1,p2):
        if not p1.endswith('/'):
            p1 += '/'
        return p1+p2

    # ---------------------------------------------------------------------------------------------------------
    # Include files depending on extension
    # Returns tuple (stat,filename):
    #  stat => False if excluded, True if included
    #  filename => The extension can be changed during upload. This is the name of the file as uploaded.
    def __checkFilteringInclude(self,sfile='',extFilterInc=''):
        if (extFilterInc != '') and (len(extFilterInc.split(',')) > 0):
            for ext in extFilterInc.split(','):
                extc = ext.split(':')
                m = re.search('\.{e}$'.format(e=extc[0]), sfile, re.I)
                if m:
                    # extension matched => include
                    if len(extc) > 1:
                        # change extension of file
                        sfile='{f}.{e}'.format(f=os.path.splitext(sfile)[0],e=extc[1])
                    return (True,sfile)
            # Nothing matched => exclude
            return (False,sfile)
        else:
            # no filtering => include
            return (True,sfile)

    # ---------------------------------------------------------------------------------------------------------
    # Exclude files depending on extension
    # False if excluded, True if included
    def __checkFilteringExclude(self,sfile='',extFilterExc=''):
        if len(extFilterExc.split(',')) > 0:
            for ext in extFilterExc.split(','):
                m = re.search('\.{e}$'.format(e=ext), sfile, re.I)
                if m:
                    # extension match => exclude
                    return False
        return True

    # ---------------------------------------------------------------------------------------------------------
    # A SQLite database can store file timestamps for each section when needed.
    # This is used for preventing upload of a file which has not changed.
    def __checkTimestampFromSQLite(self,section='',sfile=''):
        # Get file modified timestamp
        if not os.path.isfile(sfile):
            # File not found
            return False

        # Get file modification time as string
        tsfile = str(os.path.getmtime(sfile))

        # Open database (or create it if it doesn't exist)
        db = sqlite3.connect('{sid}.db'.format(sid=self.sid))
        cur = db.cursor()

        # Create table if not exist
        sql = 'CREATE TABLE IF NOT EXISTS "{t}" (filename TEXT PRIMARY KEY, ts TEXT)'.format(t=section)
        cur.execute(sql)
        db.commit()

        # Get file timestamp from db
        arr = (sfile,)
        cur.execute('SELECT ts FROM "{t}" WHERE filename=?'.format(t=section),arr)
        rows = cur.fetchall()
        if len(rows) != 1:
            # Too many matches, should only be one
            cur.execute('DELETE FROM "{t}" WHERE filename=?'.format(t=section),arr)
            db.commit()
            ts = ''
        else:
            # Get timestamp from db (as string)
            ts = str(rows[0][0])

        if ts != tsfile:
            # Different
            arr = (sfile,tsfile)
            cur.execute('REPLACE INTO "{t}" (filename,ts) VALUES (?,?)'.format(t=section),arr)
            db.commit()
            db.close()
            return True     # Indicate file is different
        else:
            # No change in timestamp
            db.close()
            return False    # Indicate file is same

    # ---------------------------------------------------------------------------------------------------------
    # Transfer all files from single "directory" to remote site "directory". 
    # After succesfull transfer move the file from "directory" to "transfered directory"
    def doType1(self,pathFrom='',pathTo='',pathTransfered='',extFilterInc='',extFilterExc=''):
        if self.conntype == 'sftp':
            self.__doType1_sftp(
                pathFrom, pathTo,
                self.__chgPath(pathTransfered),             # Check for macros
                extFilterInc, extFilterExc
            )
        else:
            self.logclass.logError('doType1 conntype not supported yet',self.lineno())

    # ---------------------------------------------------------------------------------------------------------
    # doType1 with sftp
    def __doType1_sftp(self,pathFrom='',pathTo='',pathTransfered='',extFilterInc='',extFilterExc=''):
        if not os.path.exists(pathFrom):
            self.logclass.logError('Directory does not exists : {d}'.format(d=pathFrom),self.lineno())
            return False

        # Create some directories
        if pathTransfered != '':
            if not os.path.exists(pathTransfered):
                self.logclass.logInfo('Creating directory {d}'.format(d=pathTransfered))
                os.makedirs(pathTransfered)
                if not os.path.exists(pathTransfered):
                    self.logclass.logError('Failed to create : {d}'.format(d=pathTransfered),self.lineno())
                    return False

        # Go through all files in directory
        for sfile in os.listdir(pathFrom):
            if not os.path.isdir(os.path.join(pathFrom,sfile)):
                # Check filtering (exclude overrides include filtering)
                b = self.__checkFilteringExclude(sfile,extFilterExc)
                if not b:
                    self.logclass.logInfo('File {f} excluded due to Exc filtering'.format(f=sfile))
                else:
                    (b,sfile2) = self.__checkFilteringInclude(sfile,extFilterInc)
                    if not b:
                        self.logclass.logInfo('File {f} excluded due to Inc filtering'.format(f=sfile))
                    else:
                        # Upload file
                        if self.__uploadFile_sftp(pathFrom,sfile,pathTo,sfile2):
                            if pathTransfered != '':
                                try:
                                    # Move the file to another directory
                                    self.logclass.logInfo('Move file to transfer directory')
                                    shutil.move(os.path.join(pathFrom,sfile),os.path.join(pathTransfered,sfile))
                                except Exception as e:
                                    self.logclass.logError('Failed to move file : {f}'.format(f=sfile),self.lineno())

    # ---------------------------------------------------------------------------------------------------------
    # Transfer all files from single "directory" to remote site "directory". 
    def doType2(self,section='',pathFrom='',pathTo='',extFilterInc='',extFilterExc=''):
        if self.conntype == 'sftp':
            self.__doType2_sftp(section,pathFrom,pathTo,extFilterInc,extFilterExc)
        else:
            self.logclass.logError('doType2 conntype not supported yet',self.lineno())

    # ---------------------------------------------------------------------------------------------------------
    # doType2 with sftp
    def __doType2_sftp(self,section='',pathFrom='',pathTo='',extFilterInc='',extFilterExc=''):
        if not os.path.exists(pathFrom):
            self.logclass.logError('Directory does not exists : {d}'.format(d=pathFrom),self.lineno())
            return False

        # Go through all files in directory
        for sfile in os.listdir(pathFrom):
            if not os.path.isdir(os.path.join(pathFrom,sfile)):
                # Check filtering (exclude overrides include filtering)
                b = self.__checkFilteringExclude(sfile,extFilterExc)
                if b:
                    (b,sfile2) = self.__checkFilteringInclude(sfile,extFilterInc)
                    if b:
                        # Check db regarding file if timestamp has changed
                        b = self.__checkTimestampFromSQLite(section,os.path.join(pathFrom,sfile))
                        if b:
                            # Upload file
                            self.__uploadFile_sftp(pathFrom,sfile,pathTo,sfile2)
                        #else:
                        #    self.logclass.logInfo('File {f} timestamp has not changed'.format(f=sfile))
                    #else:
                    #    self.logclass.logInfo('File {f} excluded due to Inc filtering'.format(f=sfile))
                #else:
                #    self.logclass.logInfo('File {f} excluded due to Exc filtering'.format(f=sfile))

    # ---------------------------------------------------------------------------------------------------------
    # Upload "sfile" with sftp from local "pathFrom" to remote "pathTo"
    def __uploadFile_sftp(self,pathFrom,sfile1,pathTo,sfile2):
        if self.conn == False:
            # Connect to sftp
            if not self.connect():
                return False

        if pathTo[-1] == '/':
            # remove trailing delimiter
            pathTo = pathTo[:-1]

        try:
            if self.conn.pwd != pathTo:
                # Uploading to different directory than current
                try:
                    if not self.conn.exists(pathTo):
                        self.logclass.logInfo('Create remote directory : {p}'.format(p=pathTo))
                        self.conn.makedirs(pathTo)            
                    # Next lines is actually not needed as sftp should upload to whatever directory the upload points to.
                    # For some reason got an error with a server which failed to upload the file if the current location
                    # was not the actual upload folder; too strict security? Well, this fixed it.
                    self.conn.cwd(pathTo)
                    if self.conn.pwd != pathTo:
                        self.logclass.logError('Failed to cwd to : {p}'.format(p=pathTo),self.lineno())
                        return False
                except Exception as e:
                    self.logclass.logError('Failed to create remote folder(s) : {p}'.format(p=pathTo),self.lineno())
                    return False

            # upload file
            if sfile1 != sfile2:
                # .. and renamed
                self.logclass.logInfo('Upload {f1} to {pathTo} as {f2}'.format(f1=sfile1,pathTo=pathTo,f2=sfile2))
            else:
                self.logclass.logInfo('Upload {f1} to {pathTo}'.format(f1=sfile1,pathTo=pathTo))

            # Log a start of upload
            self.logclass.logFileCreate(
                os.path.join(pathFrom,sfile1),
                '[{username}@{hostname}] {r}'.format(username=self.username,hostname=self.host,r=self.joinpath(pathTo,sfile2))
            )
            # Upload file
            self.conn.put(os.path.join(pathFrom,sfile1),self.joinpath(pathTo,sfile2))
            # Log a check of upload
            self.logclass.logFileCheck()

            # Check remote the file is actually there
            try:
                e = self.conn.stat(self.joinpath(pathTo,sfile2))
                b = (e.st_size == os.stat(os.path.join(pathFrom,sfile1)).st_size)
            except Exception as e:
                b = False
            if not b:
                self.logclass.logError('Upload failed',self.lineno())
                self.logclass.logFileFail()
                return False

            # Log a success of upload
            self.logclass.logFilePass()
            return True
        except Exception as e:
            # Upload failed
            self.logclass.logError('Caught exception: {c}: {e}'.format(c=e.__class__,e=e),self.lineno())
            self.logclass.logFileFail()
            return False

##############################################################################
## logoutput class
##############################################################################
class logoutput:
    logger = False
    sid = ''
    id = ''
    myconn = False
    maxBytes = 512000
    backupCount = 10
    debug = False

    # ---------------------------------------------------------------------------------------------------------
    # Init class
    def __init__(self,sid='',logfile='',level=logging.DEBUG):
        self.sid = sid
        self.id = 0

        self.logger = logging.getLogger(self.sid)
        hdlr = logging.handlers.RotatingFileHandler(filename=logfile, mode='a', maxBytes=self.maxBytes, backupCount=self.backupCount)
        formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
        hdlr.setFormatter(formatter)
        self.logger.addHandler(hdlr) 
        self.logger.setLevel(level)

    # ---------------------------------------------------------------------------------------------------------
    # Set debug state. Setting to True will also print to console all log messages
    def setDebug(self,d=False):
        self.debug = d

    # ---------------------------------------------------------------------------------------------------------
    # Open connection to "transfers" database
    def opendb(self,host='',user='',password='',port=3306,db='transfers'):
        if host != '':
            try:
                config = {
                    'host': host,
                    'port': port,
                    'database': db,
                    'user': user,
                    'password': password,
                    'charset': 'utf8',
                    'use_unicode': True,
                    'get_warnings': True
                }
                self.myconn = mysql.connector.Connect(**config)
            except Exception as e:
                self.logError('Caught exception: {c}: {e}'.format(c=e.__class__,e=e),self.lineno())
                self.myconn == False

    # ---------------------------------------------------------------------------------------------------------
    # Get current line in code when called
    def lineno(self):
        return inspect.currentframe().f_back.f_lineno

    # ---------------------------------------------------------------------------------------------------------
    # Close database
    def close(self):
        if not self.myconn == False:
            self.myconn.close()
            self.myconn = False

    # ---------------------------------------------------------------------------------------------------------
    # Log the message (file and/or database)
    def __logIt(self,msg,lineno=0,db=True,st=''):
        if lineno > 0:
            # We have a line number, prepend it
            msg = '[{lineno}] {msg}'.format(lineno=lineno,msg=msg)
        if self.debug:
            # Debug; print to console
            print(msg)
        if not self.logger == False:
            # Log to file
            if st == 'E':
                self.logger.error(msg)
            elif st == 'I':
                self.logger.info(msg)
            elif st == 'C':
                self.logger.critical(msg)                
            elif st == 'D':
                self.logger.debug(msg)
            elif st == 'W':
                self.logger.warning(msg)
        if (db) and (not self.myconn == False):
            # Log to database
            mycursor = self.myconn.cursor()
            mycursor.callproc('log',(st,self.sid,msg))
            mycursor.close()

    # ---------------------------------------------------------------------------------------------------------
    # Log a error message
    def logError(self,msg,lineno=0,db=True):
        self.__logIt(msg,lineno,db,'E')

    # ---------------------------------------------------------------------------------------------------------
    # Log a info message into log (& database)
    def logInfo(self,msg,lineno=0,db=True):
        self.__logIt(msg,lineno,db,'I')

    # ---------------------------------------------------------------------------------------------------------
    # Log a critical message into log (& database)
    def logCritical(self,msg,lineno=0,db=True):
        self.__logIt(msg,lineno,db,'C')

    # ---------------------------------------------------------------------------------------------------------
    # Log a debug message into log (& database)
    def logDebug(self,msg,lineno=0,db=True):
        self.__logIt(msg,lineno,db,'D')

    # ---------------------------------------------------------------------------------------------------------
    # Log a warning message into log (& database)
    def logWarning(self,msg,lineno=0,db=True):
        self.__logIt(msg,lineno,db,'W')

    # ---------------------------------------------------------------------------------------------------------
    # Log a specific event into database only:
    # Mark a upload start of file "ffile" to location "tfile"
    def logFileCreate(self,ffile,tfile):
        if not self.myconn == False:
            mycursor = self.myconn.cursor()
            results = mycursor.callproc('logfileU',(self.sid,ffile,tfile,0))
            mycursor.close()
            try:
                # Interested in id only
                self.id = int(results[3])
            except Exception as e:
                self.logError('Caught exception: {c}: {e}'.format(c=e.__class__,e=e),self.lineno())
                self.id = 0
            return True
        else:
            return False

    # ---------------------------------------------------------------------------------------------------------
    # Log a specific event into database only:
    # Mark a "checking" event created against previous filelog created with "logFileCreate"
    def logFileCheck(self):
        if (self.id > 0) and (self.myconn != False):
            mycursor = self.myconn.cursor()
            mycursor.callproc('logfileC',(self.id,))
            mycursor.close()

    # ---------------------------------------------------------------------------------------------------------
    # Log a specific event into database only:
    # Mark a "pass" of event created against previous filelog created with "logFileCreate"
    def logFilePass(self):
        if (self.id > 0) and (self.myconn != False):
            mycursor = self.myconn.cursor()
            mycursor.callproc('logfileS',(self.id,))
            mycursor.close()
            self.id = 0

    # ---------------------------------------------------------------------------------------------------------
    # Log a specific event into database only:
    # Mark a "fail" of event created against previous filelog created with "logFileCreate"
    def logFileFail(self):
        if (self.id > 0) and (self.myconn != False):
            mycursor = self.myconn.cursor()
            mycursor.callproc('logfileF',(self.id,))
            mycursor.close()
            self.id = 0
