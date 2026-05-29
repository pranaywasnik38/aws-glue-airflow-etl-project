import pysftp

cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

with pysftp.Connection(
    host="localhost",
    username="test",
    password="test",
    port=2222,
    cnopts=cnopts
) as sftp:

    print("Connected Successfully!")

    sftp.cwd('/upload')

    files = sftp.listdir()

    print(files)