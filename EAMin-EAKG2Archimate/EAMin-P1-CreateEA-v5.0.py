import win32com.client
import pandas as pd
from neo4j import GraphDatabase
import time, csv
import tkinter as tk
from tkinter import messagebox

# logging deps
import json
from pathlib import Path
from datetime import datetime, timezone
from xml.etree.ElementTree import Element, SubElement, ElementTree

# ================== Settings ==================
neo4j_uri = "bolt://localhost:7687"
neo4j_user = "neo4j"
neo4j_password = "12345678"

# database configuration
#DB_Name = 'bpic17full-1-247k'  
#DB_Name = 'bpic17full-2-1108k'
#DB_Name = 'bpic17full-3-2769k'
#DB_Name = 'bpic17full-4-13674k'
#DB_Name = 'bpic17full-5-19682k'
DB_Name = 'bpic17full-6-49167k'

# set viewpoint
#View_Name = 'Business Layer'
#View_Name = 'Product Viewpoint'
#View_Name = 'Business Process Cooperation Viewpoint'
View_Name = 'Service Realisation Viewpoint'
#View_Name = 'Layered Viewpoint'

dataSet = 'BPIC17'

driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

perf = pd.DataFrame(columns=['name', 'start', 'end', 'duration'])
start = time.time()
last = start
print('\n Start Time: took ' + str(start) + ' seconds\n')

root = tk.Tk()
root.withdraw()

# ----------------- Creation Logger -----------------
class CreationLogger:
    def __init__(self, db_name: str, view_name: str):
        #ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        #stem = f"{db_name}__{view_name.replace(' ', '_')}__{ts}"
        stem = f"{db_name}__{view_name.replace(' ', '_')}__"
        self.base = Path(".")
        output_folder = self.base / View_Name
        output_folder.mkdir(parents=True, exist_ok=True)  # create folder if it doesn't exist
        self.json_path = output_folder / f"{stem}__graph_output.json"
        self.nodes_csv = output_folder / f"{stem}__elements_created.csv"
        self.rels_csv = output_folder / f"{stem}__relations_created.csv"
        self.created_nodes = []    # dicts
        self.created_rels = []     # dicts

    def log_node(self, name, ntype, element_id=None, element_guid=None, extra=None):
        self.created_nodes.append({
            "name": name,
            "type": ntype,
            "element_id": element_id,
            "element_guid": element_guid,
            "created_at": datetime.now(timezone.utc).isoformat(),
            **(extra or {})
        })

    def log_rel(self, src_name, src_type, dst_name, dst_type, rel_type,
                connector_id=None, connector_guid=None, extra=None,
                src_id=None, dst_id=None):
        self.created_rels.append({
            "source_name": src_name,
            "source_type": src_type,
            "target_name": dst_name,
            "target_type": dst_type,
            "relation_type": rel_type,
            "connector_id": connector_id,
            "connector_guid": connector_guid,
            "src_element_id": src_id,
            "dst_element_id": dst_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            **(extra or {})
        })

    def save(self):
        # JSON summary
        data = {
            "meta": {
                "db_name": DB_Name,
                "view_name": View_Name,
                "generated_at": datetime.now(timezone.utc).isoformat()
            },
            "elements_created": self.created_nodes,
            "relations_created": self.created_rels
        }
        self.json_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

        # CSV nodes
        with self.nodes_csv.open("w", newline="", encoding="utf-8") as f:
            node_fields = ["name", "type", "element_id", "element_guid", "created_at"]
            w = csv.DictWriter(f, fieldnames=node_fields)
            w.writeheader()
            for r in self.created_nodes:
                w.writerow({k: r.get(k) for k in node_fields})

        # CSV relations
        with self.rels_csv.open("w", newline="", encoding="utf-8") as f:
            rel_fields = ["source_name","source_type","target_name","target_type",
                          "relation_type","connector_id","connector_guid",
                          "src_element_id","dst_element_id","created_at"]
            w = csv.DictWriter(f, fieldnames=rel_fields)
            w.writeheader()
            for r in self.created_rels:
                w.writerow({k: r.get(k) for k in rel_fields})

# -------- Gephi exporters (CSV + GEXF) --------
def export_for_gephi(element_by_id: dict, logger: "CreationLogger",
                     nodes_csv_path: str = View_Name + "\\" + "gephi_nodes.csv",
                     edges_csv_path: str =  View_Name + "\\" + "gephi_edges.csv",
                     gexf_path: str =  View_Name + "\\" + "graph.gexf"):

    # nodes
    nodes = []
    for neo4j_id, el in element_by_id.items():
        nodes.append({
            "Id": getattr(el, "ElementID", None),
            "Label": getattr(el, "Name", None),
            "archiType": getattr(el, "StereotypeEx", "")
        })

    # edges
    edges = []
    for r in logger.created_rels:
        src_id = r.get("src_element_id")
        dst_id = r.get("dst_element_id")
        if src_id and dst_id:
            edges.append({
                "Source": src_id,
                "Target": dst_id,
                "Type": "Directed",
                "Label": r.get("relation_type", "")
            })

    _write_csv(nodes_csv_path, nodes, ["Id","Label","archiType"])
    _write_csv(edges_csv_path, edges, ["Source","Target","Type","Label"])
    _write_gexf(gexf_path, nodes, edges)

def _write_csv(path, rows, fieldnames):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fieldnames})

def _write_gexf(path, nodes, edges):
    gexf = Element("gexf", {
        "xmlns": "http://www.gexf.net/1.2draft",
        "version": "1.2"
    })
    graph = SubElement(gexf, "graph", {
        "mode": "static",
        "defaultedgetype": "directed"
    })
    attributes = SubElement(graph, "attributes", {"class": "node", "mode": "static"})
    SubElement(attributes, "attribute", {"id": "0", "title": "archiType", "type": "string"})

    g_nodes = SubElement(graph, "nodes")
    for n in nodes:
        n_el = SubElement(g_nodes, "node", {"id": str(n["Id"]), "label": str(n.get("Label", ""))})
        attvalues = SubElement(n_el, "attvalues")
        SubElement(attvalues, "attvalue", {"for": "0", "value": str(n.get("archiType", ""))})

    g_edges = SubElement(graph, "edges")
    for idx, e in enumerate(edges):
        SubElement(g_edges, "edge", {
            "id": str(idx),
            "source": str(e["Source"]),
            "target": str(e["Target"]),
            "label": str(e.get("Label", ""))
        })
    ElementTree(gexf).write(path, encoding="utf-8", xml_declaration=True)
# ----------------------------------------------------

# instantiate logger
logger = CreationLogger(DB_Name, View_Name)

# ================== Neo4j functions ==================
def get_all_nodes():
    with driver.session(database=DB_Name) as session:
        query = """
        MATCH (n)
        WHERE n:Element_BL OR n:Element_AL
        RETURN DISTINCT id(n) AS id, n.Name AS name, coalesce(n.Type,'Unknown') AS type
        """
        result = session.run(query)
        return [(r["id"], r["name"], r["type"]) for r in result]

def get_all_rels():
    with driver.session(database=DB_Name) as session:
        query = """
        MATCH (n)-[r:Rel]->(m)
        RETURN id(n) AS id1, n.Name AS name1, coalesce(n.Type,'Unknown') AS type1,
               r.Type AS rel_type,
               id(m) AS id2, m.Name AS name2, coalesce(m.Type,'Unknown') AS type2
        """
        result = session.run(query)
        return [(rec["id1"], rec["name1"], rec["type1"],
                 rec["rel_type"],
                 rec["id2"], rec["name2"], rec["type2"]) for rec in result]

# ================== Connect to EA ==================
try:
    ea_app = win32com.client.GetActiveObject("EA.App")
    repo = ea_app.Repository
    print("Connected to EA successfully.")

    all_nodes = get_all_nodes()
    all_rels  = get_all_rels()
    print(f"Nodes fetched: {len(all_nodes)}")
    print(f"Rels fetched : {len(all_rels)}")

    model_count = repo.Models.Count
    if model_count == 0:
        raise RuntimeError("No Root Model found in EA repository.")
    root_pkg = repo.Models.GetAt(0)

    model_package = root_pkg.Packages.AddNew(View_Name +": "+ DB_Name, "")
    model_package.Update()
    root_pkg.Packages.Refresh()

    diag = model_package.Diagrams.AddNew("Business Process Diagram", "ArchiMate3::Business")
    diag.Update()
    model_package.Diagrams.Refresh()

    def get_or_create_element(pkg, name, ea_type, stereotype, neo4j_id=None, archi_type=None):
        for i in range(pkg.Elements.Count):
            el = pkg.Elements.GetAt(i)
            if el.Name == name and el.StereotypeEx == stereotype:
                return el, False
        el = pkg.Elements.AddNew(name, ea_type)
        el.StereotypeEx = stereotype
        el.Update()
        pkg.Elements.Refresh()
        logger.log_node(
            name=name,
            ntype=archi_type or ea_type,
            element_id=getattr(el, "ElementID", None),
            element_guid=getattr(el, "ElementGUID", None),
            extra={"neo4j_id": neo4j_id}
        )
        return el, True

    ARCHI_MAP = {
        "BusinessProcess":     ("BusinessProcess",     "ArchiMate3::ArchiMate_BusinessProcess"),
        "BusinessObject":      ("BusinessObject",      "ArchiMate3::ArchiMate_BusinessObject"),
        "BusinessActor":       ("BusinessActor",       "ArchiMate3::ArchiMate_BusinessActor"),
        "BusinessService":     ("BusinessService",     "ArchiMate3::ArchiMate_BusinessService"),
        "ApplicationService":  ("ApplicationService",  "ArchiMate3::ArchiMate_ApplicationService"),
        "ApplicationProcess":  ("ApplicationProcess",  "ArchiMate3::ArchiMate_ApplicationProcess"),
        "BusinessEvent":       ("ArchiMate3::BusinessEvent", "ArchiMate3::ArchiMate_BusinessEvent"),
    }

    element_by_id = {}
    COLS, DX, DY, W, H = 6, 220, 140, 100, 60
    row = col = 0
    indx=1

    for nid, name, typ in all_nodes:
        if typ not in ARCHI_MAP:
            continue
        ea_type, stereo = ARCHI_MAP[typ]
        el, created = get_or_create_element(model_package, name, ea_type, stereo,
                                            neo4j_id=nid, archi_type=typ)
        element_by_id[nid] = el
        if created:
            print(f"{indx}: Element '{name}' ({typ}) created.")
            indx += 1
        x_left = 50 + col * DX
        x_right = x_left + W
        y_top = 50 + row * DY
        y_bottom = y_top + H
        d_obj = diag.DiagramObjects.AddNew("", "")
        d_obj.ElementID = el.ElementID
        d_obj.left, d_obj.right, d_obj.top, d_obj.bottom = x_left, x_right, y_top, y_bottom
        d_obj.Update()
        col += 1
        if col >= COLS:
            col = 0
            row += 1

    diag.DiagramObjects.Refresh()

    REL_MAP = {
        "AccessRelation":      ("AccessRelation",      "Access",      "ArchiMate3::ArchiMate_Access"),
        "TriggeringRelation":  ("TriggeringRelation",  "Triggering",  "ArchiMate3::ArchiMate_Triggering"),
        "RealizationRelation": ("RealizationRelation", "Realization", "ArchiMate3::ArchiMate_Realization"),
        "ServingRelation":     ("ServingRelation",     "Serving",     "ArchiMate3::ArchiMate_Serving"),
        "FlowRelation":        ("FlowRelation",        "Flow",        "ArchiMate3::ArchiMate_Serving"),
    }
    indx2 = 1
    for id1, name1, type1, rel_type, id2, name2, type2 in all_rels:
        if id1 not in element_by_id or id2 not in element_by_id:
            continue
        src = element_by_id[id1]
        dst = element_by_id[id2]
        if rel_type not in REL_MAP:
            continue
        rel_name, ea_conn_type, stereo = REL_MAP[rel_type]
        conn = src.Connectors.AddNew(rel_name, ea_conn_type)
        conn.ClientID = src.ElementID
        conn.SupplierID = dst.ElementID
        conn.StereotypeEx = stereo
        conn.Direction = "Source - Destination"
        conn.Update()
        logger.log_rel(
            src_name=name1, src_type=type1,
            dst_name=name2, dst_type=type2,
            rel_type=rel_type,
            connector_id=getattr(conn, "ConnectorID", None),
            connector_guid=getattr(conn, "ConnectorGUID", None),
            extra={"ea_name": rel_name, "ea_type": ea_conn_type},
            src_id=src.ElementID, dst_id=dst.ElementID
        )
        print(f"{indx2}: Connector '{rel_type}' added: {name1} -> {name2}")
        indx2 += 1

    diag.Update()
    model_package.Diagrams.Refresh()
    repo.SaveDiagram(diag.DiagramID)
    repo.ReloadDiagram(diag.DiagramID)

except Exception as e:
    print(f"Error: {e}")

finally:
    try:
        logger.save()
        export_for_gephi(element_by_id, logger)
        print("\nCreated items were logged to:")
        print(f"  JSON : {logger.json_path}")
        print(f"  CSV  : {logger.nodes_csv}")
        print(f"  CSV  : {logger.rels_csv}")
        print(f"  Gephi Nodes CSV : gephi_nodes.csv")
        print(f"  Gephi Edges CSV : gephi_edges.csv")
        print(f"  Gephi GEXF      : graph.gexf")
    except Exception as log_err:
        print(f"Logging error: {log_err}")

# ================== Timing ==================
end = time.time()
new_row = pd.DataFrame([{
    'name': dataSet + '_event_import',
    'start': last,
    'end': end,
    'duration': (end - last)
}])
perf = pd.concat([perf, new_row], ignore_index=True)
print('Time: took ' + str(end - last) + ' seconds')
last = end

print('\n\nEND')
print('EAKG Time: took ' + str(last - start) + ' seconds')
