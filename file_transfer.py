import pysftp

with pysftp.Connection('10.***.***.12', username='username', password='password') as sftp:
    with sftp.cd('/home/esergly/'):             # temporarily chdir to public
#        sftp.put('/my/local/filename')  # upload file to public/ on remote
        sftp.get('msisdn')         # get a remote file