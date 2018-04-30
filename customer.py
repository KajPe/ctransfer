#!/usr/bin/python3

from sstransfer import settings,logoutput,transfer
import sys
import os
from datetime import datetime
import re

# Get full application filename
appname = os.path.abspath(sys.argv[0])

# Read settings from .ini file
s = settings(inifile=os.path.splitext(appname)[0] + '.ini')

# Get SID (used to identify logging in database)
sid = s.getString('setup','sid','SID');

# Setup logging to file
logpath = s.getString('log','path',os.path.dirname(appname))
logfile = s.getString('log','file','log.log')
if not os.path.exists(logpath):
    os.makedirs(logpath)
    if not os.path.exists(logpath):
        # Failed to create log path
        sys.exit(990)
log = logoutput(sid,os.path.join(logpath,logfile))
log.setDebug(False)     # Dont print to console

if s.sectionExists('mysql'):
    # .. and add MySQL logging
    mysqlhost = s.getString('mysql','host','');
    mysqluser = s.getString('mysql','user','');
    mysqlpasswd = s.getString('mysql','pswd','');
    mysqlport = s.getInt('mysql','port',3306);
    log.opendb(host=mysqlhost,user=mysqluser,password=mysqlpasswd,port=mysqlport)

# Start of logging
log.logInfo('Start --> (Python v{ver})'.format(ver=sys.version.split(' ')[0]))

# Get sftp options
sftpHost = s.getString('sftp','host','')
sftpUser = s.getString('sftp','user','')
sftpPassword = s.getString('sftp','password','')
sftpKey = s.getString('sftp','key','')
# Setup sftp but dont open connection yet. Connection is opened when needed.
tr = transfer(sid,log,os.path.dirname(appname))
tr.setRemote(host=sftpHost,user=sftpUser,password=sftpPassword,privkey=sftpKey)

# Set current timestamp
tr.updateTimeStamp(datetime.now())

# Read all sections from inifile
sections = sorted(s.getSections())
for section in sections:
    # We are interested in the "set-" sections only
    if section[0:4] == 'set-':
        log.logInfo('Running {k} ..'.format(k=section))

        # Get type as it tells what kind of transfer is needed
        sType = s.getString(section,'type','1')
        
        # Get some basic settings
        pathFrom = s.getString(section,'from','')
        pathTo = s.getString(section,'to','')
        extFilterInc = s.getString(section,'extfilterinc','')
        extFilterExc = s.getString(section,'extfilterexc','')

        # Now do the actual file processing accoring to sType
        if sType == '1':
            # Upload all files from single "directory" to sftp remote site "directory". 
            # After succesfull upload move the file from "directory" to "transfered directory".
            pathTransfered = s.getString(section,'transfered','')
            tr.doType1(pathFrom,pathTo,pathTransfered,extFilterInc,extFilterExc)
        elif sType == '2':
            # Transfer all files from single "directory" to remote site "directory".
            # Use a sqlite database to detect if a file has been changed and dont upload if same timestamp.
            tr.doType2(section,pathFrom,pathTo,extFilterInc,extFilterExc)            
        else:
            log.logError('Unknown type : {t}'.format(t=sType))

# Disconnect sftp
tr.disconnect()

# End logging
log.logInfo('<---- End')
