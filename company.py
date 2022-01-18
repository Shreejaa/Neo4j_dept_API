from py2neo import Graph, Node, Relationship, NodeMatcher
from flask import Flask, jsonify
from flask import abort
from flask import make_response
from flask import request
import mysql.connector as mysql
from flask_cors import CORS
import collections, functools, operator

app = Flask(__name__)
CORS(app)

@app.route('/company/departments/', methods=['GET'])
def get_department():
    g = Graph(auth=('neo4j','r123'))
    query = """
    match(d:Department) 
    return d.deptId
    """
    res = g.run(query)
    deptId = []
    for r in res:
        deptId.append(r['d.deptId'])
    deptId.sort()
    return jsonify({"departments":deptId})

@app.route('/company/employees/', methods=['GET'])
def get_employees():
    g = Graph(auth=('neo4j','r123'))
    query = """
    match(e:Employee) 
    return e.ssn
    """
    res = g.run(query)
    empid = []
    for r in res:
        empid.append(r['e.ssn'])
    empid.sort()
    return jsonify({"employees":empid})
    
@app.route('/company/projects/', methods=['GET'])
def get_projects():
    g = Graph(auth=('neo4j','r123'))
    query = """
        match (p:Project) return p.pnumber as project_number
    """
    res = g.run(query)
    pnumbers = []
    for r in res:
        pnumbers.append(int(r['project_number']))
    return jsonify({"projects": pnumbers})
    

@app.route('/company/cities/', methods=['GET'])
def get_cities():
    g = Graph(auth=('neo4j','r123'))
    query = """
    MATCH(p:Project),(dl:Department_location) return DISTINCT p.location as p, dl.address as a;
    """
    res = g.run(query)
    cities = []
    for r in res:
        if r['p'] not in cities:
            cities.append(r['p'])
        for i in r['a']:
            if i not in cities:
                cities.append(i)
    cities.sort()    
    
    return jsonify({"cities": cities})

@app.route('/company/project/<int:pno>/', methods=['GET'])
def get_pno(pno):
    g = Graph(auth=('neo4j','r123'))
    pno = str(pno)
    query = """
    MATCH(d:Department)-[controls]->(p:Project)-[worker]->(e:Employee)-[works_for]->(dd:Department), (w:WorksOn) 
    where p.pnumber='"""+pno+"""' and p.pnumber=w.pnumber and e.ssn=w.ssn 
    return DISTINCT dd.dept as dname,e.deptId as id,p.controllingDept as contDept, w.hours as hours,e.ssn as ssn,p.name as name, d.dept as n;
    """
    res = g.run(query)
    #print(res)
    ans = []
    dname = []
    id = []
    contDept = []
    name = []
    ssn = []
    n = []
    for r in res:
        ssn.append(r['ssn'])
        if r['n'] not in n:
            n.append(r['n'])
        if r['dname'] not in dname:
            dname.append({r['dname']:float(r['hours'])})
        if r['id'] not in id:
            id.append(r['id'])
        if r['contDept'] not in contDept:
            contDept.append(r['contDept'])
        if r['name'] not in name:
            name.append(r['name'])
    dept_hours = dict(functools.reduce(operator.add,
         map(collections.Counter, dname)))
    ans = {"controling_dname":n[0], 
    "controlling_dnumber":int(contDept[0]), 
    "dept_hours":dept_hours,
    "employees":ssn,
    "person_hours": sum(dept_hours.values()),
    "pname": name[0]}
    return jsonify(ans)

@app.route('/company/projects/<string:cty>/', methods=['GET'])
def get_pcity(cty):
    g = Graph(auth=('neo4j','r123'))
    query = """
    MATCH(p:Project) where p.location='"""+cty+"""' return p.name as name, p.pnumber as pnumber;
    """
    res = g.run(query)
    ans = []
    for r in res:
        ans.append({"pname":r['name'], "pnumber":r['pnumber']})
    return jsonify({"projects": ans})

@app.route('/company/departments/<string:cty>/', methods=['GET'])
def get_dcity(cty):
    g = Graph(auth=('neo4j','r123'))
    query = """
    MATCH(dl:Department_location)-[located_at]->(d:Department) where '"""+cty+"""' in dl.address return d.dept as dname, d.deptId as dnumber;
    """
    res = g.run(query)
    ans = []
    for r in res:
        ans.append({"dname":r['dname'], "dnumber":r['dnumber']})
    return jsonify({"departments": ans})

@app.route('/company/supervisees/<string:ssn>/', methods=['GET'])
def get_superviees_by_ssn(ssn):
    g = Graph(auth=('neo4j','r123'))
    result = []
    query = '''
    Match(e:Employee)-[:supervisor *0..]->(b:Employee) where e.superV="'''+ssn+'''" 
    return b.ssn;
    '''
    res = g.run(query, ssn=ssn)
    for r in res:
        result.append(r['b.ssn'])
    return jsonify({"employees":result})
    

@app.route('/company/department/<int:dno>/', methods=['GET'])
def get_department_by_id(dno):
    g = Graph(auth=('neo4j','r123'))
    project = []
    query1 = """
    match(d:Department{deptId:$dno})-[:controls]->(p:Project) 
    return p.name,p.pnumber
    """
    res1 = g.run(query1, dno=str(dno))
    for r in res1:
        project.append({"pname":r['p.name'],"pnumber":r['p.pnumber']})
    dept = []
    query2 = """
    match(d:Department{deptId:$dno})-[:managed_by]->(e:Employee) 
    return d.dept,e.first,e.last,d.date,d.ssn
    """
    res2 = g.run(query2, dno=str(dno))
    for r in res2:
        dept.append([r['d.dept'],r['e.first']+" "+r['e.last'],r['d.date'],r['d.ssn']])
    query3 = """
    match (d:Department{deptId:$dno})-[:employs]->(e:Employee) 
    return e.ssn
    """
    res3 = g.run(query3, dno=str(dno))
    empno = []
    for r in res3:
        empno.append(r['e.ssn'])
    query4 = """
    match (d:Department{deptId:$dno})-[:is_located]->(dl:Department_location) 
    return dl.address 
    """
    loc = []
    res4 = g.run(query4, dno=str(dno))
    for r in res4:
        loc = (r['dl.address'])
    dept_dno ={
        "controlled_projects": project,
        "dname": dept[0][0],
        "employees": empno,
        "locations": loc,
        "manager": dept[0][1],
        "manager_start_date": dept[0][2],
        "mgrssn" : dept[0][3]
    }
    return jsonify(dept_dno)

@app.route('/company/employee/<string:ssn>/', methods=['GET'])
def get_employee_by_ssn(ssn):
    g = Graph(auth=('neo4j','r123'))
    query1 = """
    match(e:Employee{ssn: $ssn})-[:works_for]->(d:Department) 
    return e.address,e.dob,d.dept,d.deptId,e.first,e.mid,e.last,e.sex,e.salary,e.superV
    """
    res = g.run(query1,ssn=ssn)
    result1 = []
    for r in res:
        result1.append([r['e.address'],r['e.dob'],r['d.dept'],r['d.deptId'],
        r['e.first'],r['e.sex'],r['e.last'],r['e.mid'],r['e.salary'],r['e.superV']])
    query2 = """
    match(e:Employee{ssn: $ssn})-[:dependent]->(dd:Dependent) 
    return dd.birthdate,dd.name,dd.sex,dd.relationship
    """
    res2 = g.run(query2,ssn=ssn)
    result2 = []
    for r in res2:
        result2.append({"bdate":r['dd.birthdate'],"dname":r['dd.name'],
        "gender":r['dd.sex'],"relationship":r['dd.relationship']})
    query3 = """
    match(e:Employee{ssn: $ssn})-[:works_on]->(p:Project)-[:projectWorkHours]->(w:WorksOn{ssn:$ssn})
    return w.hours,p.name,p.pnumber
    """     
    res3 = g.run(query3,ssn=ssn)
    result3 = []
    for r in res3:
        result3.append({"hours":r['w.hours'],"pname":r['p.name'],"pnumber":r['p.pnumber']})
    query4 = """
    match(e:Employee{ssn: $ssn})-[:supervisor]->(m:Employee)
     return m.ssn
    """
    res4 = g.run(query4, ssn=ssn)
    result4 = []
    for r in res4:
        result4.append(r['m.ssn'])
    query5 = """
    match(e:Employee{ssn: $ssn})-[:manages]->(d:Department) 
    return d.dept,d.deptId
    """
    res5 = g.run(query5, ssn=ssn)
    result5 = []
    for r in res5:
        result5.append([r['d.dept'],r['d.deptId']])
    emp_ssn = {
        "address": result1[0][0],
        "bdate": result1[0][1],
        "department_name": result1[0][2],
        "department_number": result1[0][3],
        "dependents": result2,
        "fname": result1[0][4],
        "gender": result1[0][5],
        "lname": result1[0][6],
        "manages": {
            "dname": result5[0][0],
            "dnumber": result5[0][1]
        },
        "minit": result1[0][7],
        "project": result3,
        "salary": result1[0][8],
        "supervisees": result4,
        "supervisor": result1[0][9]
    }
    return jsonify(emp_ssn)

if __name__ == '__main__':
    app.run(host='localhost',debug=True)