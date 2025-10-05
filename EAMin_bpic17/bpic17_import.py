# loan application

import pandas as pd
import time, csv
from neo4j import GraphDatabase


print("\n\n\n\n\n\n\n\n\n\n\n\n")
print("START START")
print("\n\n")

### begin config
# connection to Neo4J database

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "12345678")) # authentication required
#driver = GraphDatabase.driver("bolt://localhost:7687") # authentication disabled in settings
DB_Name = 'bpic17full-1-247k'  # database selection

# check connection
with driver.session(database= DB_Name) as session:
    result = session.run("RETURN 1")
    if (result.single()):
     print("\n\nConnection is OK\n\n")  # this should return a numeric 1
#driver.close()

# Neo4j can import local files only from its own import directory, see https://neo4j.com/docs/cypher-manual/current/clauses/load-csv/
# Neo4j's default configuration enables import from local file directory
#    if it is not enabled, change Neo4j'c configuration file: dbms.security.allow_csv_import_from_file_urls=true
# Neo4j's default import directory is <NEO4J_HOME>/import, 
#    to use this script
#    - EITHER change the variable path_to_neo4j_import_directory to <NEO4J_HOME>/import and move the input files to this directory
#    - OR set the import directory in Neo4j's configuration file: dbms.directories.import=
#    see https://neo4j.com/docs/cypher-manual/current/clauses/load-csv/#query-load-csv-introduction
#path_to_neo4j_import_directory = 'C:\\Temp\\Import\\'
#path_to_neo4j_import_directory = 'D:\\Users\\PC\\.Neo4jDesktop\\relate-data\\dbmss\\dbms-918afdcc-1686-454d-bde3-931c0d91b2fe\\import\\'
#path_to_neo4j_import_directory = 'D:\\Users\\PC\\.Neo4jDesktop\\relate-data\\dbmss\\dbms-76f01aa4-7ee5-417a-aa74-d2931d89fe89\\import\\'

dbms_dir = 'dbms-9551f380-b7b9-4f75-8567-5c38bfdce503'
path_to_neo4j = "C:\\Users\\PC\\.Neo4jDesktop2\\Data\\dbmss\\"
path_to_neo4j_import_directory = path_to_neo4j + dbms_dir + '\\import\\' 


# ensure to allocate enough memory to your database: dbms.memory.heap.max_size=20G advised

# the script supports loading a small sample or the full log
step_Sample = False
if(step_Sample):
    fileName = 'BPIC17sample.csv' 
    perfFileName = 'BPIC17samplePerformance.csv'
else:
    fileName = 'BPIC17full-1-247k.csv'
    perfFileName = 'BPIC17fullPerformance.csv'
    
# data model specific to BPIC17
dataSet = 'BPIC17'

#include_entities = ['Application','Workflow','Offer','Case_R','Case_AO','Case_AW','Case_WO']
include_entities = ['Application','Workflow','Offer','Case_R','Case_AO','Case_AW','Case_WO','Case_AWO']

'''
model_entities = [['Application','case', 'WHERE e.EventOrigin = "Application"'], # individual entities
                  ['Workflow', 'case', 'WHERE e.EventOrigin = "Workflow"'],
                  ['Offer', 'OfferID', 'WHERE e.EventOrigin = "Offer"'],
                  ['Case_R', 'resource', 'WHERE EXISTS(e.resource)'], # resource as entity
                  ['Case_AWO','case', 'WHERE EXISTS(e.case)']] # original case notion
'''
## my code
model_entities = [['Application','case', 'WHERE e.EventOrigin = "Application"'], # individual entities
                  ['Workflow', 'case', 'WHERE e.EventOrigin = "Workflow"'],
                  ['Offer', 'OfferID', 'WHERE e.EventOrigin = "Offer"'],
                  ['Case_R', 'resource', 'WHERE e.resource IS NOT NULL'], # resource as entity
                  ['Case_AWO','case', 'WHERE e.case IS NOT NULL']] # original case notion

# specification of derived entities: 
#    1 name of derived entity, 
#    2 name of first entity, 
#    3 name of second entity where events have an property referring to the first entity, i.e., a foreign key
#    4 name of the foreign key property by which events of the second entity refer to the first entity
model_entities_derived = [['Case_AO','Application','Offer','case'],
                          ['Case_AW','Application','Workflow','case'],
                          ['Case_WO','Workflow','Offer','case']]
    
# several steps of import, each can be switch on/off
step_ClearDB = True           # entire graph shall be cleared before starting a new import
step_LoadEventsFromCSV = False # import all (new) events from CSV file
step_FilterEvents = False       # filter events prior to graph construction
step_createLog = False         # create log nodes and relate events to log node
step_createEntities = False        # create entities from identifiers in the data as specified in this script
step_createEntityRelations = False   # create foreign-key relations between entities
step_createEntitiesDerived = False  # create derived entities as specified in the script
step_createDF = False            # compute directly-follows relation for all entities in the data
step_deleteParallelDF = False    # remove directly-follows relations for derived entities that run in parallel with DF-relations for base entities
step_createEventClasses = False  # aggregate events to event classes from data
step_createDFC = False        # aggregate directly-follows relation to event classes
step_createHOWnetwork = False   # create resource activitiy classifier and HOW network

option_filter_removeEventsWhere = 'WHERE e.lifecycle in ["SUSPEND","RESUME"]'

option_DF_entity_type_in_label = False # set to False when step_createDFC is enabled

### end config

######################################################
############# DEFAULT METHODS AND QUERIES ############
######################################################

# load data from CSV and import into graph
def LoadLog(localFile):
    datasetList = []
    headerCSV = []
    i = 0
    with open(localFile) as f:
        reader = csv.reader(f)
        for row in reader:
            if (i==0):
                headerCSV = list(row)
                i +=1
            else:
               datasetList.append(row)
        
    log = pd.DataFrame(datasetList,columns=headerCSV)
    
    return headerCSV, log

# create events from CSV table: one event node per row, one property per column
def CreateEventQuery(logHeader, fileName, LogID = ""):
    #query = f'USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM \"file:///{fileName}\" as line'
    query = f'LOAD CSV WITH HEADERS FROM \"file:///{fileName}\" as line'
    for col in logHeader:
        if col == 'idx':
            # original code
        #    column = f'toInt(line.{col})'
            column = f'line.{col}'
        elif col in ['timestamp','start','end']:
            column = f'datetime(line.{col})'
        else:
            column = 'line.'+col
        newLine = ''
        if (logHeader.index(col) == 0 and LogID != ""):
            newLine = f' MERGE (e:Event {{Log: "{LogID}",{col}: {column},'
        elif (logHeader.index(col) == 0):
            newLine = f' MERGE (e:Event {{ {col}: {column},'
        else:
            newLine = f' {col}: {column},'
        if (logHeader.index(col) == len(logHeader)-1):
            newLine = f' {col}: {column} }})'
            
        query = query + newLine
    # my code
    query = f'LOAD CSV WITH HEADERS FROM \"file:///{fileName}\" as line '
    query = query +  'MERGE (e:Event {Log: "BPIC17",idx: line.idx, case: line.case, Activity: line.Activity, timestamp: datetime(line.timestamp), lifecycle: line.lifecycle, ApplicationType: line.ApplicationType, LoanGoal: line.LoanGoal, RequestedAmount: line.RequestedAmount, MonthlyCost: line.MonthlyCost, resource: line.resource, Selected: line.Selected, EventID: line.EventID, OfferID: line.OfferID, FirstWithdrawalAmount: line.FirstWithdrawalAmount, Action: line.Action, Accepted: line.Accepted, CreditScore: line.CreditScore, NumberOfTerms: line.NumberOfTerms, EventOrigin: line.EventOrigin, OfferedAmount: line.OfferedAmount, EventIDraw: line.EventIDraw })'
           
    return query;

# run query for Neo4J database
def runQuery(driver, query):
    with driver.session(database= DB_Name) as session:
        result = session.run(query).single()
        if result != None: 
            return result.value()
        else:
            return None
        
def filterEvents(tx, condition):
    qFilterEvents = f'MATCH (e:Event) {condition} DELETE e'
    print(qFilterEvents)
    tx.run(qFilterEvents)
        
def add_log(tx, log_id):
    qCreateLog = f'MERGE (:Log {{ID: "{log_id}" }})'
    print(qCreateLog)
    tx.run(qCreateLog)

    qLinkEventsToLog = f'''
            MATCH (e:Event {{Log: "{log_id}" }}) 
            MATCH (l:Log {{ID: "{log_id}" }}) 
            MERGE (l)-[:L_E]->(e)'''
    print(qLinkEventsToLog)
    tx.run(qLinkEventsToLog)

def create_entity(tx, entity_type, entity_id, WHERE_event_property):
    qCreateEntity = f'''
            MATCH (e:Event) {WHERE_event_property}
            WITH e.{entity_id} AS id
            MERGE (en:Entity {{ID:id, uID:("{entity_type}"+ toString(id)), EntityType:"{entity_type}" }})'''
    print(qCreateEntity)
    tx.run(qCreateEntity)
    
def correlate_events_to_entity(tx, entity_type, entity_id, WHERE_event_property):
    qCorrelate = f'''
            MATCH (e:Event) {WHERE_event_property}
            MATCH (n:Entity {{EntityType: "{entity_type}" }}) WHERE e.{entity_id} = n.ID
            MERGE (e)-[:E_EN]->(n)'''
    print(qCorrelate)
    tx.run(qCorrelate)
    
def create_entity_derived_from2(tx, derived_entity_type, entity_type1, entity_type2, fk_2to1):
    qCreateEntity = f'''
            MATCH (e1:Event) -[:E_EN]-> (n1:Entity) WHERE n1.EntityType="{entity_type1}"
            MATCH (e2:Event) -[:E_EN]-> (n2:Entity) WHERE n2.EntityType="{entity_type2}" AND n1 <> n2 AND e2.{fk_2to1} = n1.ID 
            WITH DISTINCT n1.ID as n1_id, n2.ID as n2_id
            WHERE n1_id <> "Unknown" AND n2_id <> "Unknown"
            MERGE ( :Entity {{ {entity_type1}ID: n1_id, {entity_type2}ID: n2_id, EntityType : "{derived_entity_type}", uID :  '{derived_entity_type}_'+toString(n1_id)+'_'+toString(n2_id) }} )'''
    print(qCreateEntity)
    tx.run(qCreateEntity)
    
def correlate_events_to_entity_derived2(tx, derived_entity_type, entity_type1, entity_type2):
    qCorrelate1 = f'''
        MATCH ( e1 : Event ) -[:E_EN]-> (n1:Entity) WHERE n1.EntityType="{entity_type1}"
        MATCH ( derived : Entity ) WHERE derived.EntityType = "{derived_entity_type}" AND n1.ID = derived.{entity_type1}ID
        MERGE ( e1 ) -[:E_EN]-> ( derived )'''
    print(qCorrelate1)
    tx.run(qCorrelate1)
    qCorrelate2 = f'''
        MATCH ( e2 : Event ) -[:E_EN]-> (n2:Entity) WHERE n2.EntityType="{entity_type2}"
        MATCH ( derived : Entity ) WHERE derived.EntityType = "{derived_entity_type}" AND n2.ID = derived.{entity_type2}ID
        MERGE ( e2 )  -[:E_EN]-> ( derived )'''
    print(qCorrelate2)
    tx.run(qCorrelate2)
    
def createDirectlyFollows(tx, entity_type, option_DF_entity_type_in_label):
    qCreateDF = f'''
        MATCH ( n : Entity ) WHERE n.EntityType="{entity_type}"
        MATCH ( n ) <-[:E_EN]- ( e )
        
        WITH n , e as nodes ORDER BY e.timestamp,ID(e)
        WITH n , collect ( nodes ) as nodeList
        UNWIND range(0,size(nodeList)-2) AS i
        WITH n , nodeList[i] as first, nodeList[i+1] as second'''
    qCreateDF = qCreateDF  + '\n'
    
    if option_DF_entity_type_in_label == True:
        qCreateDF = qCreateDF  + f'MERGE ( first ) -[df:DF_{entity_type}]->( second )'
    else:
        qCreateDF = qCreateDF  + f'MERGE ( first ) -[df:DF {{EntityType:n.EntityType}} ]->( second )'

    print(qCreateDF)
    tx.run(qCreateDF)
    
def deleteParallelDirectlyFollows_Derived(tx, derived_entity_type, original_entity_type):
    if option_DF_entity_type_in_label == True:
        qDeleteDF = f'''
            MATCH (e1:Event) -[df:DF_{derived_entity_type}]-> (e2:Event)
            WHERE (e1:Event) -[:DF_{original_entity_type}]-> (e2:Event)
            DELETE df'''
    else:
        qDeleteDF = f'''
            MATCH (e1:Event) -[df:DF {{EntityType: "{derived_entity_type}" }}]-> (e2:Event)
            WHERE (e1:Event) -[:DF {{EntityType: "{derived_entity_type}" }}]-> (e2:Event)
            DELETE df'''

    print(qDeleteDF)
    tx.run(qDeleteDF)     
    
    
def createEventClass_Activity(tx):
    qCreateEC = f'''
        MATCH ( e : Event ) WITH distinct e.Activity AS actName
        MERGE ( c : Class {{ Name:actName, Type:"Activity", ID: actName}})'''
    print(qCreateEC)
    tx.run(qCreateEC)
        
    qLinkEventToClass = f'''
        MATCH ( c : Class ) WHERE c.Type = "Activity"
        MATCH ( e : Event ) WHERE c.Name = e.Activity
        merge ( e ) -[:E_C]-> ( c )'''
    print(qLinkEventToClass)
    tx.run(qLinkEventToClass)
    
    
def createEventClass_ActivityANDLifeCycle(tx):
    qCreateEC = f'''
        MATCH ( e : Event ) WITH distinct e.Activity AS actName,e.lifecycle AS lifecycle
        MERGE ( c : Class {{ Name:actName, Lifecycle:lifecycle, Type:"Activity+Lifecycle", ID: actName+"+"+lifecycle}})'''
    print(qCreateEC)
    tx.run(qCreateEC)
        
    qLinkEventToClass = f'''
        MATCH ( c : Class ) WHERE c.Type = "Activity+Lifecycle"    
        MATCH ( e : Event ) where e.Activity = c.Name AND e.lifecycle = c.Lifecycle
        merge ( e ) -[:E_C]-> ( c )'''
    print(qLinkEventToClass)
    tx.run(qLinkEventToClass)
    
def createEventClass_Resource(tx):
    qCreateEC = f'''
        MATCH ( e : Event ) WITH distinct e.resource AS name
        MERGE ( c : Class {{ Name:name, Type:"Resource", ID: name}})'''
    print(qCreateEC)
    tx.run(qCreateEC)
        
    qLinkEventToClass = f'''
        MATCH ( e : Event )
        MATCH ( c : Class ) WHERE c.Type = "Resource" AND c.ID = e.resource
        merge ( e ) -[:E_C]-> ( c )'''
    print(qLinkEventToClass)
    tx.run(qLinkEventToClass)

def aggregateAllDFrelations(tx):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDFC = f'''
        MATCH ( c1 : Class ) <-[:E_C]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:E_C]-> ( c2 : Class )
        MATCH (e1) -[:E_EN] -> (n)_]()
