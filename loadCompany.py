import sys
import csv
from py2neo import Graph, Node, Relationship, NodeMatcher
 

def loadEmployees(g, file):
   with open(file, 'r') as f:
       rows = list(csv.reader(f))
   for row in rows:
       r = ",".join(row).split(":")
       n = Node("Employee", first=r[0], mid=r[1], last=r[2], ssn=r[3], dob=r[4], address=r[5], sex=r[6],
         salary=int(r[7]), superV=r[8], deptId=int(r[9]))
       g.create(n)

def loadDepatments(g, file):
   with open(file,'r') as f:
       rows = list(csv.reader(f))
   for row in rows:
       r = row[0].split(":")
       print(r)
       n = Node('Department',dept=r[0], deptId=r[1], ssn=r[2], date=r[3])
       g.create(n)

def loadDepatments2(g, file):
   with open(file,'r') as f:
       rows = list(csv.reader(f))
   matcher = NodeMatcher(g)
   for row in rows:
       r = row[0].split(":")
       d = matcher.match("Department", ssn=r[2]).first()
       e = matcher.match("Employee", ssn=r[2]).first()
       #print(d)
       #n = Node('Departments',dept=r[0], deptId=r[1], date=r[3])
       rel1 = Relationship(d,"managed_by",e)
       rel2 = Relationship(e,"manages",d)
       g.create(rel1)
       g.create(rel2)
 
def loadEmployees2(g, file):
    with open(file, 'r') as f:
        rows = list(csv.reader(f))
    matcher = NodeMatcher(g)
    for row in rows:
        r = ",".join(row).split(":")
        if r[8] == "null":
            d = matcher.match("Department", deptId=r[9]).first()
            e = matcher.match("Employee", ssn=r[3]).first()
            rel1 = Relationship(d,"employs",e)
            rel2 = Relationship(e,"works_for",d)
            g.create(rel1)
            g.create(rel2)
        elif r[8] != "null":
            d = matcher.match("Department", deptId=r[9]).first()
            boss = matcher.match("Employee", ssn=r[8]).first()
            e = matcher.match("Employee", ssn=r[3]).first()
            rel1 = Relationship(d,"employs",e)
            rel2 = Relationship(e,"works_for",d)
            rel3 = Relationship(e,"supervisee",boss)
            rel4 = Relationship(boss,"supervisor",e)
            g.create(rel1)
            g.create(rel2)
            g.create(rel3)
            g.create(rel4)


def loadDependents(g, file):
    with open(file,'r') as f:
       rows = list(csv.reader(f))
    matcher = NodeMatcher(g)
    for row in rows:
       r = row[0].split(":")
       print(r)
       d = Node('Dependent',ssn=r[0], name=r[1], sex=r[2], birthdate=r[3], relationship=r[4])
       e = matcher.match("Employee", ssn=r[0]).first()
       rel1 = Relationship(d,"dependents_of",e)
       rel2 = Relationship(e,"dependent",d)
       g.create(rel1)
       g.create(rel2)

def loadProject(g, file):
   with open(file, 'r') as f:
       rows = list(csv.reader(f))
   matcher = NodeMatcher(g)
   for row in rows:
       r = row[0].split(":")
       print(r)
       p = Node("Project", name=r[0], pnumber=r[1], location=r[2], controllingDept=r[3])
       d = matcher.match("Department", deptId=r[3]).first()
       e = matcher.match("Employee", deptId=r[3]).first()
       rel1 = Relationship(d,"controls",p)
       rel2 = Relationship(p,"controlled_by",d)
       g.create(rel1)
       g.create(rel2)

def loadDeptLocations(g, file):
   with open(file,'r') as f:
       rows = list(csv.reader(f))
   matcher = NodeMatcher(g)
   for row in rows:
       r = row[0].split(":")
       rlen = len(r)
       d = matcher.match('Department',deptId=r[0]).first()
       n = Node('Department_location', deptId=r[0], address=','.join(r[1:rlen]).split(','))
       rel = Relationship(d,"is_located",n)
       rel1 = Relationship(n,"located_at",d)
       g.create(rel)
       g.create(rel1)

def loadWorksOn(g, file):
    with open(file, 'r') as f:
        rows = list(csv.reader(f))
    matcher = NodeMatcher(g)
    for row in rows:
        r = row[0].split(":")
        w = Node("WorksOn", ssn=r[0], pnumber=r[1], hours=r[2])
        p = matcher.match("Project", pnumber=r[1]).first()
        e = matcher.match("Employee",ssn=r[0]).first()
        rel1 = Relationship(e,"empWorkHours",w)
        rel2 = Relationship(p,"projectWorkHours",w)
        g.create(rel1)
        g.create(rel2)

def loadWorksOn2(g, file):
    with open(file, 'r') as f:
        rows = list(csv.reader(f))
    matcher = NodeMatcher(g)
    for row in rows:
        r = row[0].split(":")
        e = matcher.match('Employee',ssn=r[0]).first()
        p = matcher.match("Project", pnumber=r[1]).first()
        rel1 = Relationship(p,"worker",e)
        rel2 = Relationship(e,"works_on",p)
        g.create(rel1)
        g.create(rel2)


def main():
    g = Graph(auth=('neo4j','Adventure@25'))
    g.delete_all()
    loadEmployees(g,"./"+sys.argv[1]+"/EMPLOYEES.dat")
    loadDepatments(g,"./"+sys.argv[1]+"/DEPARTMENTS.dat")
    loadDepatments2(g,"./"+sys.argv[1]+"/DEPARTMENTS.dat")
    loadEmployees2(g,"./"+sys.argv[1]+"/EMPLOYEES.dat")
    loadDependents(g,"./"+sys.argv[1]+"/DEPENDENTS.dat")
    loadDeptLocations(g,"./"+sys.argv[1]+"/DEPT_LOCATIONS.dat")
    loadProject(g,"./"+sys.argv[1]+"/PROJECTS.dat")
    loadWorksOn(g,"./"+sys.argv[1]+"/WORKS_ON.dat")
    loadWorksOn2(g,"./"+sys.argv[1]+"/WORKS_ON.dat")

main()