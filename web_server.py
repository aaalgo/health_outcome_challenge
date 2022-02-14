#!/usr/bin/env python3
import os
import subprocess as sp
import pickle
import time
from flask import Flask, request, send_file, jsonify, Response
from flask import Blueprint, render_template
from flask_compress import Compress
import cms
import config
#import s3_data
import local_db as s3_data
#from inf_server import inference



loader = cms.RawCaseLoader()
normer = cms.CaseNormalizer()

# creates a Flask application, named app
app = Blueprint('aaalgo_cms', __name__, static_folder='web/dist/static')

@app.route('/')
def index ():
    return send_file('index.html')

@app.route('/user/login', methods=['GET', 'POST'])
def user_login ():
    return jsonify({'code': 20000, 'data': 'token'})

@app.route('/user/logout', methods=['GET', 'POST'])
def user_logout ():
    return jsonify({'code': 20000, 'data': 'token'})

@app.route('/user/info')
def user_info ():
    info = {
            'roles': ['admin'],
            'introduction': ['hello'],
            'avatar': 'https://avatars2.githubusercontent.com/u/5427210?s=460&v=4',
            'name': 'Super Admin'

    }
    return jsonify({'code': 20000, 'data': info})

def field_to_dict (f, level):
    if type(f) is list:
        if level > 0:
            return ''
        return [recursive_to_dict(x, level + 1) for x in f]
    return f

def recursive_to_dict (case, level):
    return {key: field_to_dict(case[i], level) for i, key in enumerate(case._fields)}

def case_to_dict (case):
    return recursive_to_dict(case, 0)

def fake_int (v):
    return 0

def fake_float (v):
    return 0

def fake_str (s):
    return 'xxxx'

def fake_v (v):
    if v is None:
        return v
    if type(v) is int:
        return fake_int(v)
    if type(v) is float:
        return fake_float(v)
    if type(v) is str:
        return fake_str(v)
    print(v)
    assert False

def fake (data):
    faked = {}
    for k, v in data.items():
        if type(v) is list:
            l2 = []
            for r in v:
                r2 = {}
                for k2, v2 in r.items():
                    r2[k2] = fake_v(v2)
                    pass
                l2.append(r2)
                pass
            v = l2
        else:
            v = fake_v(v)
            pass
        faked[k] = v
        pass
    return faked

@app.route('/api/raw/')
def api_raw_case ():
    pid = request.args.get('pid', '176814441')
    return s3_data.load_raw_case(pid)

@app.route('/api/case/')
def api_case ():
    pid = request.args.get('pid', '176814441')
    data = s3_data.load_raw_case(pid)
    #print(resp.headers)
    #print(type(data))
    raw = loader.load(data.decode('ascii'))
    case = normer.apply(raw)
    tables = []
    for i, field in enumerate(case._fields):
        if i == 0: continue
        exclude = ['DSYSRTKY']
        table = case[i]
        if len(table) == 0:
            columns = []
        else:
            columns = [l for l in table[0]._fields if not l in exclude]
        tables.append({'key': i, 'label': field, 'columns': columns})
    data = case_to_dict(case)
    if config.USE_FAKE_DATA:
        data = fake(data)
    return jsonify({'code': 20000, 'meta': tables, 'data': data})



@app.route('/api/inference/')
def api_inference ():
    pid = request.args.get('pid', '483248201')
    date = request.args.get('date', '483248201')
    result = inference.delay(pid, date)
    while True:
        if result.ready():
            break
        print('waiting...')
        time.sleep(1)
    result = result.get()
    pdf_path = result['pdf_path']
    with open(pdf_path, 'rb') as f:
        pdf = f.read()
    resp = Response(pdf)
    resp.headers['Content-Disposition'] = "inline; filename=%s" % os.path.basename(cache_path)
    resp.mimetype = 'application/pdf'
    return resp

@app.route('/api/meta/')
def api_meta ():
    specs = normer.specs
    meta = []
    key = 0
    for ctype, tables, fields, _ in specs:
        columns = []
        columns.append('table')
        columns.append('name')
        for i, table in enumerate(tables):
            columns.append('v%d_table' % (i+1))
            columns.append('v%d_column' % (i+1))
            pass
        columns.append('desc')

        data = []
        for field_name, subfields, versions, lines, _ in fields:
            if subfields is None:
                dic = {'table': '',
                       'name': field_name,
                       'desc': lines}
                for i, (t, v) in enumerate(zip(tables, versions)):
                    dic['v%d_table' % (i+1)] = t
                    dic['v%d_column' % (i+1)] = v[0]
                    pass
                data.append(dic)
            else:
                '''
                dic = {'table': '',
                       'name': field_name,
                       'desc': ''}
                for i, (t, v) in enumerate(zip(tables, versions)):
                    dic['v%d_table' % (i+1)] = ''
                    dic['v%d_column' % (i+1)] = ''
                    pass
                data.append(dic)
                '''

                for i, sf in enumerate(subfields):
                    #for i, (t, v) in enumerate(zip(table, versions)):
                    dic = {'table': field_name,
                           'name': sf,
                           'desc': lines[i]}
                    for j, (t, v) in enumerate(zip(tables, versions)):
                        name, names = v
                        if not name is None:
                            dic['v%d_table' % (j+1)] = name
                        else:
                            dic['v%d_table' % (j+1)] = t
                        if not names is None:
                            dic['v%d_column' % (j+1)] = names[i]
                        else:
                            dic['v%d_column' % (j+1)] = ''
                        pass
                    data.append(dic)
                pass
        meta.append({
                     'key': key,
                     'ctype': ctype,
                     'columns': columns,
                     'data': data})
        key += 1
        pass
    return jsonify({'code': 20000, 'meta': meta})

@app.route('/api/stats/')
def api_stats ():
    with open('stats.pkl', 'rb') as f:
        data = pickle.load(f)
    return jsonify({'code': 20000, 'data': data})

# run the application
if __name__ == "__main__":
    compress = Compress()
    bp = app
    app = Flask(__name__)
    # 如果跑到服务器后头，需要用prefix=/demo
    #app.register_blueprint(bp, url_prefix='/demo')
    app.register_blueprint(bp, url_prefix='/')
    compress.init_app(app)
    app.run(host='0.0.0.0', port=16666, debug=True)
    pass
