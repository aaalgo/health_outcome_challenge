from minio import Minio
import cms
import config

s3 = None

def load_raw_case_real (pid):
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
    #print(type(resp.data))
    return resp.data

def load_raw_case (pid):
    if config.USE_FAKE_DATA:
        with open('demo_case', 'rb') as f:
            return f.read()
        pass
    return load_raw_case_real(pid)

