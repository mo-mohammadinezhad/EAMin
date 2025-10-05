import pandas as pd
import time, csv
from neo4j import GraphDatabase
import datetime

print("\n\n\n\n\n\n\n\n\n\n\n\n")
print("START START")
print("\n\n")

### begin config
dataSet = 'BPIC17'

step_ClearEAKG = True
Step_CreateBusinessProcess = True
Step_CreateBusinessObjects = True
Step_CreateBusinessActors =  True
Step_CreateBusinessService = True
Step_CreateApplicationService = True
Step_CreateApplicationProcess = True
Step_CreateBusinessEvents = True
Step_CreateLinkProcessActor = True
Step_CreateLinkProcessBusinessObject = True
Step_CreateLinkBetweenProcess =True
Step_CreateLinkBusinessProcessBusinessService = True
Step_CreateLinkBusinessProcessApplicationService = True
Step_CreateLinkApplicationProcessApplicationService = True
Step_CreateLinkBusinessEventBusinessProcess =True



# Set the number of paths of DF_C
# For large datasets, this value should be higher
Rel_Count = 10

ProcessList=""

# connection to Neo4J database
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "12345678"))  # authentication required
# driver = GraphDatabase.driver("bolt://localhost:7687")  # authentication disabled in settings

# database configuration
#DB_Name = 'bpic17full-1-247k'  
#DB_Name = 'bpic17full-2-1108k'
#DB_Name = 'bpic17full-3-2769k'
#DB_Name = 'bpic17full-4-13674k'
#DB_Name = 'bpic17full-5-19682k'
DB_Name = 'bpic17full-6-49167k'

# set enterprise architecture viewpoint
#Viewpoint_Name = 'Business Layer'
#Viewpoint_Name = 'Product Viewpoint'
#Viewpoint_Name = 'Business Process Co-operation Viewpoint'
Viewpoint_Name = 'Service Realisation Viewpoint'
#Viewpoint_Name = 'Layered Viewpoint'


# check connection
with driver.session(database=DB_Name) as session:
    result = session.run("RETURN 1")
    if result.single():
        print("\n\nConnection is OK\n\n")  # this should return a numeric 1

# run query for Neo4J database
def runQuery(driver, query, params=None):
    with driver.session(database=DB_Name) as session:
        # If parameters are provided, pass them to the query
        if params:
            result = session.run(query, params).single()
        else:
            #result = session.run(query).single()
            result = session.run(query)
        
        if result is not None:
            return result.value()
        else:
            return None


perf = pd.DataFrame(columns=['name', 'start', 'end', 'duration'])
start = time.time()
last = start
print('\n Start Time: took ' + str(start) + ' seconds\n')

current_time = datetime.datetime.now()
# Display time in HH:MM:SS.mmm format
current_time_ms = current_time.strftime("%H:%M:%S") + f".{current_time.microsecond // 1000:03d}"
print("Current Time with Milliseconds:", current_time_ms)




def ReturnProcessList():
    q = '''
    match(n:Element_BL{Type: "BusinessProcess"}) 
    return n.Name 
    '''
    print(q)
    
    ProcessList = runQuery(driver, q)
    return ProcessList


## Clear Neo4j
if step_ClearEAKG:
    q='''
    MATCH p=()-[:Rel]->()   DETACH DELETE p
    UNION
    MATCH (n:Element_BL)  DETACH DELETE n
    UNION
    MATCH (n1:Element_AL) DETACH DELETE n1
    '''
    runQuery(driver, q)
    print(q)






#####################################################################
### Business Process Creation
#####################################################################
if  Step_CreateBusinessProcess:
    q = '''
    MATCH (n:Class)-[r:DF_C]->(n1:Class)
    WITH DISTINCT r.EntityType AS entity
    MATCH (n:Class {Type: 'Activity+Lifecycle'})
    OPTIONAL MATCH (n)<-[r:DF_C {EntityType: entity}]-(m)
    WHERE r.count > $Rel_Count
    MATCH (n)-[:DF_C {EntityType: entity}]->(p)
    WHERE m IS NULL AND p IS NOT NULL
    MERGE (ea:Element_BL{Name: n.Name, Type: "BusinessProcess", Layer: "Business", Viewpoint: $Viewpoint_Name})
    '''
    # Pass Rel_Count parameter to the query
    print(q)
    runQuery(driver, q, params={'Rel_Count': Rel_Count,'Viewpoint_Name': Viewpoint_Name})
    ProcessList= ReturnProcessList()

   

# Create BusinessObject
if Step_CreateBusinessObjects:

 for Item in ProcessList:
        
        q1 = '''
        MATCH (e:Event {Activity: $item})-[:E_EN]-(en:Entity)
        WITH DISTINCT en.EntityType AS name
        MERGE (ea:Element_BL {Name: name, Viewpoint: $Viewpoint_Name})
        ON CREATE SET 
        ea.Type = "BusinessObject",
        ea.Layer = "Business",
        ea.RelatedProcess = [$item]   // initial value as a list
        ON MATCH SET 
        ea.RelatedProcess = 
            CASE 
            WHEN NOT $item IN ea.RelatedProcess 
            THEN ea.RelatedProcess + $item   // add only if it does not already exist
            ELSE ea.RelatedProcess
            END
        '''
        runQuery(driver, q1, params={'item': Item,'Viewpoint_Name': Viewpoint_Name})




#################################################################
#########Creating BusinessActors
if Step_CreateBusinessActors:
    #process= ReturnProcessList()
    for Item in ProcessList:
        q1 = '''
        MATCH (e:Event {Activity: $item})
        WITH e.resource AS resource, COUNT(e.resource) AS resourceCount ORDER BY resourceCount DESC
        LIMIT 4
        MERGE (ea:Element_BL{Name: resource, Type: "BusinessActor", EventName:$item ,Layer: "Business", Viewpoint: $Viewpoint_Name})
        '''
        runQuery(driver, q1, params={'item': Item,'Viewpoint_Name': Viewpoint_Name})
       
        # Pass Rel_Count parameter to the query
        print(q)
    

#####################################################################
### BusinessService Creation
if Step_CreateBusinessService:
    for Item in ProcessList:
        q = '''
        MERGE (ea:Element_BL{Name: $item, Type: "BusinessService", RelatedProcess: $item, Layer: "Business", Viewpoint: $Viewpoint_Name})
        '''
        print(q)
        runQuery(driver, q, params={'item': Item,'Viewpoint_Name': Viewpoint_Name})


####################################################################
### Creating a ApplicationService
if Step_CreateApplicationService:
    if Viewpoint_Name != 'Business Layer':
        for Item in ProcessList:
            q = '''
            MERGE (ea:Element_AL{Name: $item, Type: "ApplicationService", Layer: "Application", Viewpoint: $Viewpoint_Name});
            '''
            runQuery(driver, q, params={'item': Item,'Viewpoint_Name': Viewpoint_Name}) 
            print(q)

####################################################################
### Creating a ApplicationProcess
if Step_CreateApplicationProcess:
    if Viewpoint_Name != 'Business Layer':
        for Item in ProcessList:
            q = '''
            MERGE (ea:Element_AL{Name: $item, Type: "ApplicationProcess", Layer: "Application", Viewpoint: $Viewpoint_Name});
            '''
            runQuery(driver, q, params={'item': Item,'Viewpoint_Name': Viewpoint_Name}) 
            print(q)

######################################################################
### Creating a Business event
if Step_CreateBusinessEvents:
    for Item in ProcessList:
        q = '''
        MERGE (ea:Element_BL{Name: $item, Type: "BusinessEvent", RelatedProcess: $item, Layer: "Business", Viewpoint: $Viewpoint_Name})
        '''
        print(q)
        runQuery(driver, q, params={'item': Item,'Viewpoint_Name': Viewpoint_Name})


##########################################################
## Creating a link between processes and the actor
if Step_CreateLinkProcessActor:
    Q1 = '''
    MATCH (BActor:Element_BL{Type: "BusinessActor"})
    with BActor as ea1
    Match(ea2:Element_BL{Name: ea1.EventName, Type: "BusinessProcess"})
    MERGE (ea1)-[:Rel{Name: "TriggeringRelation" , Type: "TriggeringRelation"}]->(ea2)
    '''
    print(Q1)
    runQuery(driver, Q1)


################################################################
## Creating a link between processes and the BusinessObject
if Step_CreateLinkProcessBusinessObject:

    q = '''
    MATCH (ea1:Element_BL {Type: "BusinessObject"})
    UNWIND ea1.RelatedProcess AS procName
    MATCH (ea2:Element_BL {Name: procName, Type: "BusinessProcess"})
    MERGE (ea2)-[:Rel {Name: "AccessRelation", Type: "AccessRelation"}]->(ea1);
    '''
    print(q)
    runQuery(driver, q)


################################################################
### Creating a link between processes
if Step_CreateLinkBetweenProcess:
    q = '''
    MATCH (ea1:Element_BL{Name: "A_Create Application", Type: "BusinessProcess"})
    MATCH (ea2:Element_BL{Name: "O_Create Offer", Type: "BusinessProcess"})
    MERGE (ea1)-[:Rel{Name: 'FlowRelation' , Type: 'FlowRelation'}]->(ea2)
    UNION
    MATCH (ea3:Element_BL{Name: "W_Validate application", Type: "BusinessProcess"})
    MERGE (ea2)-[:Rel{Name: 'FlowRelation' , Type: 'FlowRelation'}]->(ea3);
    '''
    print(q)
    runQuery(driver, q)



### Creating a link between BusinessService and BusinessProcess
if Step_CreateLinkBusinessProcessBusinessService:
    q = '''
    MATCH (ea2:Element_BL{Type: "BusinessService" })
    MATCH (ea1:Element_BL{Name: ea2.RelatedProcess , Type: "BusinessProcess" })
    MERGE (ea1)-[:Rel{Name: "RealizationRelation" , Type: "RealizationRelation"}]->(ea2)
    '''
    print(q)
    runQuery(driver, q)


    

#####################################################################
## Creating a link between ApplicationService and BusinessProcess
if Step_CreateLinkBusinessProcessApplicationService:
    q = '''
    MATCH (ea1:Element_BL{Type: "BusinessProcess" })
    MATCH (ea2:Element_AL{Name:ea1.Name, Type: "ApplicationService" })
    MERGE (ea1)-[:Rel{Name: "ServingRelation" , Type: "ServingRelation"}]->(ea2);
    '''
    print(q)
    runQuery(driver, q)

#####################################################################
## Creating a link between ApplicationProcess and ApplicationService
if Step_CreateLinkApplicationProcessApplicationService:
    q = '''
    MATCH (ea1:Element_AL{Type: "ApplicationProcess" })
    MATCH (ea2:Element_AL{Name:ea1.Name, Type: "ApplicationService" })
    MERGE (ea1)-[:Rel{Name: "RealizationRelation" , Type: "RealizationRelation"}]->(ea2);
    '''
    print(q)
    runQuery(driver, q)
    
 
#######################################################################
## Creating a link between Business event and BusinessProcess
if Step_CreateLinkBusinessEventBusinessProcess:
    q = '''
    MATCH (ea1:Element_BL{Type: "BusinessEvent"})
    MATCH (ea2:Element_BL{Name: ea1.RelatedProcess , Type: "BusinessProcess"})
    MERGE (ea1)-[:Rel{Name: "TriggeringRelation" , Type: "TriggeringRelation"}]->(ea2);
    '''
    print(q)
    runQuery(driver, q)






########################################################
end = time.time()
# Create a new DataFrame for the new row
new_row = pd.DataFrame([{
    'name': dataSet + '_event_import',
    'start': last,
    'end': end,
    'duration': (end - last)
}])
# Use concat to append the new row to perf
perf = pd.concat([perf, new_row], ignore_index=True)
print('Time: took ' + str(end - last) + ' seconds')
last = end

print('\n\n' + 'END')
print('EAKG Time: took ' + str(last - start) + ' seconds')

current_time = datetime.datetime.now()
current_time_ms = current_time.strftime("%H:%M:%S") + f".{current_time.microsecond // 1000:03d}"
print("\n END Time with Milliseconds:", current_time_ms)
