import re
import sys
import os.path
import ifcopenshell
import itertools
import json
from neo4j import GraphDatabase

def chunks2(iterable,size,filler=None):
    it = itertools.chain(iterable,itertools.repeat(filler,size-1))
    chunk = tuple(itertools.islice(it,size))
    while len(chunk) == size:
        yield chunk
        chunk = tuple(itertools.islice(it,size))

class IfcTypeDict(dict):
    def __missing__(self, key):
        value = self[key] = ifcopenshell.create_entity(key).wrapped_data.get_attribute_names()
        return value

typeDict = IfcTypeDict()


""" try:
 assert typeDict["IfcWall"] == ('GlobalId', 'OwnerHistory', 'Name', 'Description', 'ObjectType', 'ObjectPlacement', 'Representation', 'Tag')
except AssertionError:
  pass """


need_classes = ["IfcFlowSegment",
"IfcFlowMovingDevice",
"IfcCovering",
"IfcSite",
"IfcBuilding",
"IfcBuildingStorey",
"IfcFlowSegment",
"IfcRelAggregates",
"IfcRelContainedInSpatialStructure",
"IfcProject",
"IfcBuildingElementProxy",
"IfcPropertySet",
"IfcAirTerminalBoxType",
"IfcAirTerminalType",
"IfcAirToAirHeatRecoveryType",
"IfcBoilerType",
"IfcChillerType",
"IfcCoilType",
"IfcCompressorType",
"IfcCondenserType",
"IfcCooledBeamType",
"IfcCoolingTowerType",
"IfcDamperType",
"IfcDuctFittingType",
"IfcDuctSegmentType",
"IfcDuctSilencerType",
"IfcEvaporativeCoolerType",
"IfcEvaporatorType",
"IfcFanType",
"IfcFilterType",
"IfcFlowMeterType",
"IfcGasTerminalType",
"IfcHeatExchangerType",
"IfcHumidifierType",
"IfcPipeFittingType",
"IfcPipeSegmentType",
"IfcPumpType",
"IfcSpaceHeaterType",
"IfcTankType",
"IfcTubeBundleType",
"IfcUnitaryEquipmentType",
"IfcValveType",
"IfcVibrationIsolatorType",
"IfcDerivedUnitElement",
"IfcDerivedUnit",
"IfcMaterial",
"IfcOwnerHistory",
"IfcSIUnit",
"IfcUnitAssignment",
"IfcRelAssociatesMaterial",
"IfcRelConnectsPorts",
"IfcRelCoversBldgElements",
"IfcRelServicesBuildings",
"IfcRelAssociatesClassification",
"IfcPipeSegmentType",
"IfcRelDefinesByProperties",
"IfcRelDefinesByType",
"IfcPropertySingleValue",
"IfcRelDefinesByProperties"]

need_classes = list(set(need_classes))


nodes = []
edges = []
#wallid = None

#print("Start")

#ourLabel = sys.argv[2]
ourLabel = "LOD100"


#f = ifcopenshell.open(sys.argv[1])
f = ifcopenshell.open("C:\Temp\ifc_files\triilium_rvt_mep.ifc")
for el in f:
  #print(type(el))
  tid = el.id()
  cls = el.is_a()
  pairs = []
  keys = []
  if cls in need_classes:
    try:
      keys = [x for x in el.get_info() if x not in ["type", "id"]]
    except RuntimeError:
      # we actually can't catch this, but try anyway
      pass   

      
    for i in range(len(el)):
    
      val = el[i]
      if any(hasattr(val,"is_a") and val.is_a(thisTyp) for thisTyp in ["IfcBoolean", "IfcLabel", "IfcText", "IfcReal","IfcVolumetricFlowRateMeasure","IfcPressureMeasure","IfcLengthMeasure","IfcAreaMeasure","IfcPositiveLengthMeasure","IfcVolumeMeasure","IfcIdentifier","IfcInteger","IfcLogical","IfcSoundPowerMeasure"]):
        val = val.wrappedValue
      if type(val) not in (str, bool, float):
        continue
      pairs.append((keys[i], val))
    
    nodes.append((tid, cls, pairs))
    for i in range(len(el)): 
      try:
        el[i]
      except RuntimeError as e:
        if str(e) != "Entity not found":
          print("ID", tid, e, file=sys.stderr)
        continue

      if isinstance(el[i], ifcopenshell.entity_instance):
        if el[i].id() != 0:
          edges.append((tid, el[i].id(), typeDict[cls][i]))
          continue
        else:
          print("attribute " + typeDict[cls][i] + " of " + str(tid) + " is zero", file=sys.stderr)

      try:
        iter(el[i])
      except TypeError:
        continue
      destinations = [x.id() for x in el[i] if isinstance(x, ifcopenshell.entity_instance)]
      for connectedTo in destinations:
        edges.append((tid, connectedTo, typeDict[cls][i]))
  

if len(nodes) == 0:
  print("no nodes in file", file=sys.stderr)
  sys.exit(1)

#print(nodes)

indexes = set(["nid", "cls"])

#data=open("C:\\Temp\\store_Ifccomp_info.txt",'w+')

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "rain"))
session = driver.session()

q1 = ""
nid_lst = []
for chunk in chunks2(nodes, 100):
  idx = 0
  #print("CREATE ", end="")
  #q1 = q1 + "CREATE "
  for i in chunk:
    if i is None: 
      continue
    nId, cls, pairs = i
    nid_lst.append(nId)
    idx = idx + 1

    pairsStr = ""
    for k,v in pairs:
      indexes.add(k)
      pairsStr += ", " + k + ": " + json.dumps(v)

    #print("(a" + str(idx) + ":" + ourLabel + " { nid: " + str(nId) + ",cls: '" + cls + "'" + pairsStr + " })", end="")
    q1 = "CREATE " + "(a" + str(idx) + ":" + ourLabel + " { nid: " + str(nId) + ",cls: '" + cls + "'" + pairsStr + " });"  
    session.run(q1)
    #print(q1,file=data)
    q1 = ""

  #print(";")
  #q1 = q1 + ";"

""" print("Nodes list nID:",file = data)
print(nid_lst,file=data)

print("Edges:",file = data)
print(edges,file=data)

print("Nodes:",file = data)
print(nodes,file=data) """


q1 = ""  
#print(q1)
#nodes = session.run(q1)
#for idxName in indexes:
  #q1 = "CREATE INDEX on :" + ourLabel + "(" + idxName + ");"
  #session.run(q1)
  #print(q1,file=data)
  #q1 = ""

q2 = ""

print("Match Started")

for (nId1, nId2, relType) in edges:
  #print(""" MATCH (a:{:s}),(b:{:s}) WHERE a.nid = {:d} AND b.nid = {:d} CREATE (a)-[r:{:s}]->(b) RETURN r; """.format(ourLabel, ourLabel, nId1, nId2, relType))
  if nId2 in nid_lst:
    q2 = "MATCH " + "(a" + ":" + ourLabel + ")," + "(b" + ":" + ourLabel + ")" + " WHERE a.nid = " + " " + str(nId1) + " AND " + "b.nid = " + " " + str(nId2) + " CREATE (a)-[r:" + relType + "]->(b) RETURN r;"
    session.run(q2)
    #print(q2,file=data)
    q2 = ""
  else:
    #print("Skipped:",nId2,file=data)
    continue

#data.close()

#nodes2 = session.run(q2)
#print(q2)



#print("MATCH a=(first:IfcNode {nid: " + str(wallid) + "})-[:RELTYPE*1..2]-(other {cls: \"IFCWINDOW\"}) RETURN distinct other;")#