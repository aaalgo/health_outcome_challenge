import os
import time
import subprocess as sp
from celery import Celery
import cms
import config
import s3_data

app = Celery('inf_server', backend=config.CELERY_BACKEND, broker=config.CELERY_BROKER)

s3 = None


def load_fake_case (pid):
    return None

def load_raw_case (pid):
    #if config.USE_FAKE_DATA:
    #    return load_fake_case(pid)
    global s3
    if s3 is None:
        s3 = Minio(config.S3_SERVER,
              access_key=config.S3_ACCESS_KEY,
              secret_key=config.S3_SECRET_KEY,
              secure=False)
        pass
    resp = s3.get_object(config.S3_BUCKET, config.s3_key(pid))
    print(resp.headers)
    print(type(resp.data))
    return resp.data

@app.task
def inference(pid, date):
    #pid = request.args.get('pid', '483248201')
    #date = request.args.get('date', '483248201')
    cache_path = os.path.abspath(os.path.join('cache', '%s_%s.pdf' % (pid, date)))
    if not os.path.exists(cache_path):
        data = s3_data.load_raw_case(pid)
        data_path = os.path.abspath(os.path.join('cache', '%s.raw' % pid))
        with open(data_path, 'wb') as f:
            f.write(b'%d\t' % int(pid))
            f.write(data)
        sp.check_call('cd CMScode/report; python3 report.py %s 20120101 %s' % (data_path, cache_path), shell=True)
        pass
    with open(cache_path, 'rb') as f:
        pdf = f.read()
    return {'pdf_path': cache_path}

