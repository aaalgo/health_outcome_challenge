#!/usr/bin/env python3
from jinja2 import Template

TMPL_HTML = """<html>
<body>
{% for loss, label, predict, cutoff, demo, claims, codes, d_demo, d_claims, d_codes, case in cases %}
<hr/>
<table border="1">
<tr><th>pid</th><th>loss</th><th>label</th><th>prediction</th><th>cutoff</th></tr>
<tr><td>{{case.pid}}</td><td>{{loss}}</td><td>{{label}}</td><td>{{predict}}</td><td>{{cutoff}}</td></tr>
</table>
<br/>
<table border="1">
    <tr>
    {% for c in demo_columns %}
    <td>{{c}}</td>
    {% endfor %}
    </tr>
    {% for demo in case.demos() %}
    <tr>
        {% for c in demo_columns %}
            <td>{{ demo[c]}}</td>
        {% endfor %}
    </tr>
    {% endfor %}
</table>
<table border="1">
    <tr>
    {% for c in claim_columns %}
    <td>{{c}}</td>
    {% endfor %}
    </tr>
    {% for claim in case.claims() %}
    <tr>
        {% for c in claim_columns %}
            <td>{{ claim[c] }}</td>
        {% endfor %}
    </tr>
    {% endfor %}
</table>

<table border="1">
    <tr>
    {% for c in claim_features %}
    <td>{{c[:6]}}</td>
    {% endfor %}
    </tr>
    {% for claim in d_claims %}
    <tr>
        {% for c in claim %}
            <td>
            {% if c > 0.0001 or c < -0.0001 %}
                {{ '%0.4f' % c }}
            {% endif %}
            </td>
        {% endfor %}
    </tr>
    {% endfor %}
</table>


{% endfor %}


</body>

</html>"""

tmpl = Template(TMPL_HTML)




def generate_report (loader, cases, path):
    with open(path, 'w') as f:
        f.write(tmpl.render({
            'cases': cases,
            'demo_columns': loader.demo_columns(),
            'claim_columns': loader.claim_columns(),
            'claim_features': loader.feature_columns()[:-100],
        }))
        pass

if __name__ == '__main__':
    import cms
    import pickle
    loader = cms.CoreLoader(cms.loader)
    with open('eval.pkl', 'rb') as f:
        out = pickle.load(f)
        pass
    cases = []
    for pid, loss, label, predict, cutoff, X, grads, line in out:
        assert not line is None
        case = loader.load(line, True)
        assert pid == case.pid
        #case.label = int(label)
        #case.predict = predict
        demo, claims, codes, mapping = X
        d_demo, d_claims, d_codes = grads
        cases.append((loss, label, predict, cutoff, demo[0], claims[0], codes[0], d_demo[0], d_claims[0], d_codes, case))
        pass

    generate_report(loader, cases, 'eval.html')

