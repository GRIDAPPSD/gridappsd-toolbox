"""Module for querying and parsing SPARQL through GridAPPS-D"""
import logging
import pandas as pd
import numpy as np
import re
from gridappsd import GridAPPSD, topics, utils

class SPARQLManager:
    """Class for querying and parsing SPARQL in GridAPPS-D.
    """

    
    def __init__(self, gapps, feeder_mrid, model_api_topic, simulation_id=None, timeout=30):
        """Connect to the platform.

        :param feeder_mrid: unique identifier for the feeder in
            question. Since PyVVO works on a per feeder basis, this is
            required, and all queries will be executed for the specified
            feeder.
        :param gapps: gridappsd_object
        :param timeout: timeout for querying the blazegraph database.
        """

        # Connect to the platform.
        self.gad = gapps
       
        # Assign feeder mrid.
        self.feeder_mrid = feeder_mrid

        # Timeout for SPARQL queries.
        self.timeout = timeout

        # Powergridmodel API topic
        self.topic = model_api_topic

        # Assign simulation id
        self.simulation_id = simulation_id


    def PerLengthPhaseImpedance_line_names(self):
        LINES_QUERY = """
        PREFIX r:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX c:  <http://iec.ch/TC57/CIM100#>
        SELECT ?line_name ?bus1 ?bus2 ?length ?line_config ?phase
        WHERE {
        VALUES ?fdrid {"%s"}
         ?s r:type c:ACLineSegment.
         ?s c:Equipment.EquipmentContainer ?fdr.
         ?fdr c:IdentifiedObject.mRID ?fdrid.
         ?s c:IdentifiedObject.name ?line_name.
         ?s c:Conductor.length ?length.
         ?s c:ACLineSegment.PerLengthImpedance ?lcode.
         ?lcode r:type c:PerLengthPhaseImpedance.
         ?lcode c:IdentifiedObject.name ?line_config.
         ?t1 c:Terminal.ConductingEquipment ?s.
         ?t1 c:Terminal.ConnectivityNode ?cn1.
         ?t1 c:ACDCTerminal.sequenceNumber "1".
         ?cn1 c:IdentifiedObject.name ?bus1.
         ?t2 c:Terminal.ConductingEquipment ?s.
         ?t2 c:Terminal.ConnectivityNode ?cn2.
         ?t2 c:ACDCTerminal.sequenceNumber "2".
         ?cn2 c:IdentifiedObject.name ?bus2.
         OPTIONAL {?acp c:ACLineSegmentPhase.ACLineSegment ?s.
           ?acp c:ACLineSegmentPhase.phase ?phsraw.
           ?acp c:ACLineSegmentPhase.sequenceNumber ?seq.
             bind(strafter(str(?phsraw),"SinglePhaseKind.") as ?phase)}
        }
        ORDER BY ?line_name ?phase
        """% self.feeder_mrid

        results = self.gad.query_data(LINES_QUERY)
        bindings = results['data']['results']['bindings']
        return bindings


    def PerLengthPhaseImpedance_line_configs(self):
        VALUES_QUERY = """
        PREFIX r:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX c:  <http://iec.ch/TC57/CIM100#>
        SELECT DISTINCT ?line_config ?count ?row ?col ?r_ohm_per_m ?x_ohm_per_m ?b_S_per_m WHERE {
        VALUES ?fdrid {"%s"}
         ?eq r:type c:ACLineSegment.
         ?eq c:Equipment.EquipmentContainer ?fdr.
         ?fdr c:IdentifiedObject.mRID ?fdrid.
         ?eq c:ACLineSegment.PerLengthImpedance ?s.
         ?s r:type c:PerLengthPhaseImpedance.
         ?s c:IdentifiedObject.name ?line_config.
         ?s c:PerLengthPhaseImpedance.conductorCount ?count.
         ?elm c:PhaseImpedanceData.PhaseImpedance ?s.
         ?elm c:PhaseImpedanceData.row ?row.
         ?elm c:PhaseImpedanceData.column ?col.
         ?elm c:PhaseImpedanceData.r ?r_ohm_per_m.
         ?elm c:PhaseImpedanceData.x ?x_ohm_per_m.
         ?elm c:PhaseImpedanceData.b ?b_S_per_m
        }
        ORDER BY ?line_config ?row ?col
        """% self.feeder_mrid

        results = self.gad.query_data(VALUES_QUERY)
        bindings = results['data']['results']['bindings']
        return bindings


    def PerLengthSequenceImpedance_line_names(self):
        LINES_QUERY = """
        PREFIX r:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX c:  <http://iec.ch/TC57/CIM100#>
        SELECT ?line_name ?bus1 ?bus2 ?length ?line_config
        WHERE {
        VALUES ?fdrid {"%s"}
         ?s r:type c:ACLineSegment.
         ?s c:Equipment.EquipmentContainer ?fdr.
         ?fdr c:IdentifiedObject.mRID ?fdrid.
         ?s c:IdentifiedObject.name ?line_name.
         ?s c:Conductor.length ?length.
         ?s c:ACLineSegment.PerLengthImpedance ?lcode.
         ?lcode r:type c:PerLengthSequenceImpedance.
         ?lcode c:IdentifiedObject.name ?line_config.
         ?t1 c:Terminal.ConductingEquipment ?s.
         ?t1 c:Terminal.ConnectivityNode ?cn1.
         ?t1 c:ACDCTerminal.sequenceNumber "1".
         ?cn1 c:IdentifiedObject.name ?bus1.
         ?t2 c:Terminal.ConductingEquipment ?s.
         ?t2 c:Terminal.ConnectivityNode ?cn2.
         ?t2 c:ACDCTerminal.sequenceNumber "2".
         ?cn2 c:IdentifiedObject.name ?bus2
        }
        ORDER BY ?line_name
        """% self.feeder_mrid

        results = self.gad.query_data(LINES_QUERY)
        bindings = results['data']['results']['bindings']
        return bindings


    def PerLengthSequenceImpedance_line_configs(self):
        VALUES_QUERY = """
        PREFIX r:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX c:  <http://iec.ch/TC57/CIM100#>
        SELECT DISTINCT ?line_config ?r1_ohm_per_m ?x1_ohm_per_m ?b1_S_per_m ?r0_ohm_per_m ?x0_ohm_per_m ?b0_S_per_m WHERE {
        VALUES ?fdrid {"%s"}
         ?eq r:type c:ACLineSegment.
         ?eq c:Equipment.EquipmentContainer ?fdr.
         ?fdr c:IdentifiedObject.mRID ?fdrid.
         ?eq c:ACLineSegment.PerLengthImpedance ?s.
         ?s r:type c:PerLengthSequenceImpedance.
         ?s c:IdentifiedObject.name ?line_config.
         ?s c:PerLengthSequenceImpedance.r ?r1_ohm_per_m.
         ?s c:PerLengthSequenceImpedance.x ?x1_ohm_per_m.
         ?s c:PerLengthSequenceImpedance.bch ?b1_S_per_m.
         ?s c:PerLengthSequenceImpedance.r0 ?r0_ohm_per_m.
         ?s c:PerLengthSequenceImpedance.x0 ?x0_ohm_per_m.
         ?s c:PerLengthSequenceImpedance.b0ch ?b0_S_per_m
        }
        ORDER BY ?line_config
        """% self.feeder_mrid

        results = self.gad.query_data(VALUES_QUERY)
        bindings = results['data']['results']['bindings']
        return bindings


    def ACLineSegment_line_names(self):
        LINES_QUERY = """
        PREFIX r:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX c:  <http://iec.ch/TC57/CIM100#>
        SELECT ?line_name ?basev ?bus1 ?bus2 ?length ?r1_Ohm ?x1_Ohm ?b1_S ?r0_Ohm ?x0_Ohm ?b0_S
        WHERE {
        VALUES ?fdrid {"%s"}
         ?s r:type c:ACLineSegment.
         ?s c:Equipment.EquipmentContainer ?fdr.
         ?fdr c:IdentifiedObject.mRID ?fdrid.
         ?s c:IdentifiedObject.name ?line_name.
         ?s c:ConductingEquipment.BaseVoltage ?bv.
         ?bv c:BaseVoltage.nominalVoltage ?basev.
         ?s c:Conductor.length ?length.
         ?s c:ACLineSegment.r ?r1_Ohm.
         ?s c:ACLineSegment.x ?x1_Ohm.
         OPTIONAL {?s c:ACLineSegment.bch ?b1_S.}
         OPTIONAL {?s c:ACLineSegment.r0 ?r0_Ohm.}
         OPTIONAL {?s c:ACLineSegment.x0 ?x0_Ohm.}
         OPTIONAL {?s c:ACLineSegment.b0ch ?b0_S.}
         ?t1 c:Terminal.ConductingEquipment ?s.
         ?t1 c:Terminal.ConnectivityNode ?cn1.
         ?t1 c:ACDCTerminal.sequenceNumber "1".
         ?cn1 c:IdentifiedObject.name ?bus1.
         ?t2 c:Terminal.ConductingEquipment ?s.
         ?t2 c:Terminal.ConnectivityNode ?cn2.
         ?t2 c:ACDCTerminal.sequenceNumber "2".
         ?cn2 c:IdentifiedObject.name ?bus2
        }
        GROUP BY ?line_name ?basev ?bus1 ?bus2 ?length ?r1_Ohm ?x1_Ohm ?b1_S ?r0_Ohm ?x0_Ohm ?b0_S
        ORDER BY ?line_name
        """% self.feeder_mrid

        results = self.gad.query_data(LINES_QUERY)
        bindings = results['data']['results']['bindings']
        return bindings


    def WireInfo_line_names(self):
        LINES_QUERY = """
        PREFIX r:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX c:  <http://iec.ch/TC57/CIM100#>
        SELECT ?line_name ?basev ?bus1 ?bus2 ?length ?wire_spacing_info ?phase ?wire_cn_ts ?wireinfo
        WHERE {
        VALUES ?fdrid {"%s"}
         ?s r:type c:ACLineSegment.
         ?s c:Equipment.EquipmentContainer ?fdr.
         ?fdr c:IdentifiedObject.mRID ?fdrid.
         ?s c:IdentifiedObject.name ?line_name.
         ?s c:ConductingEquipment.BaseVoltage ?bv.
         ?bv c:BaseVoltage.nominalVoltage ?basev.
         ?s c:Conductor.length ?length.
         ?s c:ACLineSegment.WireSpacingInfo ?inf.
         ?inf c:IdentifiedObject.name ?wire_spacing_info.
         ?t1 c:Terminal.ConductingEquipment ?s.
         ?t1 c:Terminal.ConnectivityNode ?cn1.
         ?t1 c:ACDCTerminal.sequenceNumber "1".
         ?cn1 c:IdentifiedObject.name ?bus1.
         ?t2 c:Terminal.ConductingEquipment ?s.
         ?t2 c:Terminal.ConnectivityNode ?cn2.
         ?t2 c:ACDCTerminal.sequenceNumber "2".
         ?cn2 c:IdentifiedObject.name ?bus2.
         ?acp c:ACLineSegmentPhase.ACLineSegment ?s.
         ?acp c:ACLineSegmentPhase.phase ?phsraw.
          bind(strafter(str(?phsraw),"SinglePhaseKind.") as ?phase)
         ?acp c:ACLineSegmentPhase.WireInfo ?phinf.
         ?phinf c:IdentifiedObject.name ?wire_cn_ts.
         ?phinf a ?phclassraw.
          bind(strafter(str(?phclassraw),"CIM100#") as ?wireinfo)
        }
        ORDER BY ?line_name ?phase
        """% self.feeder_mrid

        results = self.gad.query_data(LINES_QUERY)
        bindings = results['data']['results']['bindings']
        return bindings


    def WireInfo_spacing(self):
        LINES_QUERY = """
        PREFIX r:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX c:  <http://iec.ch/TC57/CIM100#>
        SELECT DISTINCT ?wire_spacing_info ?cable ?usage ?bundle_count ?bundle_sep ?seq ?xCoord ?yCoord
        WHERE {
        VALUES ?fdrid {"%s"}
         ?eq r:type c:ACLineSegment.
         ?eq c:Equipment.EquipmentContainer ?fdr.
         ?fdr c:IdentifiedObject.mRID ?fdrid.
         ?eq c:ACLineSegment.WireSpacingInfo ?w.
         ?w c:IdentifiedObject.name ?wire_spacing_info.
          bind(strafter(str(?w),"#") as ?id)
         ?pos c:WirePosition.WireSpacingInfo ?w.
         ?pos c:WirePosition.xCoord ?xCoord.
         ?pos c:WirePosition.yCoord ?yCoord.
         ?pos c:WirePosition.sequenceNumber ?seq.
         ?w c:WireSpacingInfo.isCable ?cable.
         ?w c:WireSpacingInfo.phaseWireCount ?bundle_count.
         ?w c:WireSpacingInfo.phaseWireSpacing ?bundle_sep.
         ?w c:WireSpacingInfo.usage ?useraw.
          bind(strafter(str(?useraw),"WireUsageKind.") as ?usage)
        }
        ORDER BY ?wire_spacing_info ?seq
        """% self.feeder_mrid

        results = self.gad.query_data(LINES_QUERY)
        bindings = results['data']['results']['bindings']
        return bindings


    def WireInfo_overhead(self):
        LINES_QUERY = """
        PREFIX r:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX c:  <http://iec.ch/TC57/CIM100#>
        SELECT DISTINCT ?wire_cn_ts ?radius ?coreRadius ?gmr ?rdc ?r25 ?r50 ?r75 ?amps
        WHERE {
        VALUES ?fdrid {"%s"}
         ?eq r:type c:ACLineSegment.
         ?eq c:Equipment.EquipmentContainer ?fdr.
         ?fdr c:IdentifiedObject.mRID ?fdrid.
         ?acp c:ACLineSegmentPhase.ACLineSegment ?eq.
         ?acp c:ACLineSegmentPhase.WireInfo ?w.
         ?w r:type c:OverheadWireInfo.
         ?w c:IdentifiedObject.name ?wire_cn_ts.
         ?w c:WireInfo.radius ?radius.
         ?w c:WireInfo.gmr ?gmr.
         OPTIONAL {?w c:WireInfo.rDC20 ?rdc.}
         OPTIONAL {?w c:WireInfo.rAC25 ?r25.}
         OPTIONAL {?w c:WireInfo.rAC50 ?r50.}
         OPTIONAL {?w c:WireInfo.rAC75 ?r75.}
         OPTIONAL {?w c:WireInfo.coreRadius ?coreRadius.}
         OPTIONAL {?w c:WireInfo.ratedCurrent ?amps.}
        }
        ORDER BY ?wire_cn_ts
        """% self.feeder_mrid

        results = self.gad.query_data(LINES_QUERY)
        bindings = results['data']['results']['bindings']
        return bindings


    def WireInfo_concentricNeutral(self):
        LINES_QUERY = """
        PREFIX r:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX c:  <http://iec.ch/TC57/CIM100#>
        SELECT DISTINCT ?wire_cn_ts ?radius ?coreRadius ?gmr ?rdc ?r25 ?r50 ?r75 ?amps ?insulation ?insulation_thickness ?diameter_core ?diameter_insulation ?diameter_screen ?diameter_jacket ?diameter_neutral ?sheathneutral ?strand_count ?strand_radius ?strand_gmr ?strand_rdc
        WHERE {
        VALUES ?fdrid {"%s"}
         ?eq r:type c:ACLineSegment.
         ?eq c:Equipment.EquipmentContainer ?fdr.
         ?fdr c:IdentifiedObject.mRID ?fdrid.
         ?acp c:ACLineSegmentPhase.ACLineSegment ?eq.
         ?acp c:ACLineSegmentPhase.WireInfo ?w.
         ?w r:type c:ConcentricNeutralCableInfo.
         ?w c:IdentifiedObject.name ?wire_cn_ts.
         ?w c:WireInfo.radius ?radius.
         ?w c:WireInfo.gmr ?gmr.
         OPTIONAL {?w c:WireInfo.rDC20 ?rdc.}
         OPTIONAL {?w c:WireInfo.rAC25 ?r25.}
         OPTIONAL {?w c:WireInfo.rAC50 ?r50.}
         OPTIONAL {?w c:WireInfo.rAC75 ?r75.}
         OPTIONAL {?w c:WireInfo.coreRadius ?coreRadius.}
         OPTIONAL {?w c:WireInfo.ratedCurrent ?amps.}
         OPTIONAL {?w c:WireInfo.insulationMaterial ?insraw.
           bind(strafter(str(?insraw),"WireInsulationKind.") as ?insmat)}
         OPTIONAL {?w c:WireInfo.insulated ?insulation.}
         OPTIONAL {?w c:WireInfo.insulationThickness ?insulation_thickness.}
         OPTIONAL {?w c:CableInfo.diameterOverCore ?diameter_core.}
         OPTIONAL {?w c:CableInfo.diameterOverJacket ?diameter_jacket.}
         OPTIONAL {?w c:CableInfo.diameterOverInsulation ?diameter_insulation.}
         OPTIONAL {?w c:CableInfo.diameterOverScreen ?diameter_screen.}
         OPTIONAL {?w c:CableInfo.sheathAsNeutral ?sheathneutral.}
         OPTIONAL {?w c:ConcentricNeutralCableInfo.diameterOverNeutral ?diameter_neutral.}
         OPTIONAL {?w c:ConcentricNeutralCableInfo.neutralStrandCount ?strand_count.}
         OPTIONAL {?w c:ConcentricNeutralCableInfo.neutralStrandGmr ?strand_gmr.}
         OPTIONAL {?w c:ConcentricNeutralCableInfo.neutralStrandRadius ?strand_radius.}
         OPTIONAL {?w c:ConcentricNeutralCableInfo.neutralStrandRDC20 ?strand_rdc}
        }
        ORDER BY ?wire_cn_ts
        """% self.feeder_mrid

        results = self.gad.query_data(LINES_QUERY)
        bindings = results['data']['results']['bindings']
        return bindings


    def WireInfo_tapeShield(self):
        LINES_QUERY = """
        PREFIX r:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX c:  <http://iec.ch/TC57/CIM100#>
        SELECT DISTINCT ?wire_cn_ts ?radius ?coreRadius ?gmr ?rdc ?r25 ?r50 ?r75 ?amps ?insulation ?insulation_thickness ?diameter_core ?diameter_insulation ?diameter_screen ?diameter_jacket ?sheathneutral ?tapelap ?tapethickness
        WHERE {
        VALUES ?fdrid {"%s"}
         ?eq r:type c:ACLineSegment.
         ?eq c:Equipment.EquipmentContainer ?fdr.
         ?fdr c:IdentifiedObject.mRID ?fdrid.
         ?acp c:ACLineSegmentPhase.ACLineSegment ?eq.
         ?acp c:ACLineSegmentPhase.WireInfo ?w.
         ?w r:type c:TapeShieldCableInfo.
         ?w c:IdentifiedObject.name ?wire_cn_ts.
         ?w c:WireInfo.radius ?radius.
         ?w c:WireInfo.gmr ?gmr.
         OPTIONAL {?w c:WireInfo.rDC20 ?rdc.}
         OPTIONAL {?w c:WireInfo.rAC25 ?r25.}
         OPTIONAL {?w c:WireInfo.rAC50 ?r50.}
         OPTIONAL {?w c:WireInfo.rAC75 ?r75.}
         OPTIONAL {?w c:WireInfo.coreRadius ?coreRadius.}
         OPTIONAL {?w c:WireInfo.ratedCurrent ?amps.}
         OPTIONAL {?w c:WireInfo.insulationMaterial ?insraw.
           bind(strafter(str(?insraw),"WireInsulationKind.") as ?insmat)}
         OPTIONAL {?w c:WireInfo.insulated ?insulation.}
         OPTIONAL {?w c:WireInfo.insulationThickness ?insulation_thickness.}
         OPTIONAL {?w c:CableInfo.diameterOverCore ?diameter_core.}
         OPTIONAL {?w c:CableInfo.diameterOverJacket ?diameter_jacket.}
         OPTIONAL {?w c:CableInfo.diameterOverInsulation ?diameter_insulation.}
         OPTIONAL {?w c:CableInfo.diameterOverScreen ?diameter_screen.}
         OPTIONAL {?w c:CableInfo.sheathAsNeutral ?sheathneutral.}
         OPTIONAL {?w c:TapeShieldCableInfo.tapeLap ?tapelap.}
         OPTIONAL {?w c:TapeShieldCableInfo.tapeThickness ?tapethickness.}
        }
        ORDER BY ?wire_cn_ts
        """% self.feeder_mrid

        results = self.gad.query_data(LINES_QUERY)
        bindings = results['data']['results']['bindings']
        return bindings


    def PowerTransformerEnd_xfmr_names(self):
        XFMRS_QUERY = """
        PREFIX r:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX c:  <http://iec.ch/TC57/CIM100#>
        SELECT ?xfmr_name ?vector_group ?end_number ?bus ?base_voltage ?connection ?ratedS ?ratedU ?r_ohm ?angle ?grounded ?r_ground ?x_ground
        WHERE {
        VALUES ?fdrid {"%s"}
         ?p c:Equipment.EquipmentContainer ?fdr.
         ?fdr c:IdentifiedObject.mRID ?fdrid.
         ?p r:type c:PowerTransformer.
         ?p c:IdentifiedObject.name ?xfmr_name.
         ?p c:PowerTransformer.vectorGroup ?vector_group.
         ?end c:PowerTransformerEnd.PowerTransformer ?p.
         ?end c:TransformerEnd.endNumber ?end_number.
         ?end c:PowerTransformerEnd.ratedS ?ratedS.
         ?end c:PowerTransformerEnd.ratedU ?ratedU.
         ?end c:PowerTransformerEnd.r ?r_ohm.
         ?end c:PowerTransformerEnd.phaseAngleClock ?angle.
         ?end c:PowerTransformerEnd.connectionKind ?connraw.
          bind(strafter(str(?connraw),"WindingConnection.") as ?connection)
         ?end c:TransformerEnd.grounded ?grounded.
         OPTIONAL {?end c:TransformerEnd.rground ?r_ground.}
         OPTIONAL {?end c:TransformerEnd.xground ?x_ground.}
         ?end c:TransformerEnd.Terminal ?trm.
         ?trm c:Terminal.ConnectivityNode ?cn.
         ?cn c:IdentifiedObject.name ?bus.
         ?end c:TransformerEnd.BaseVoltage ?bv.
         ?bv c:BaseVoltage.nominalVoltage ?base_voltage.
        }
        ORDER BY ?xfmr_name ?end_number
        """% self.feeder_mrid

        results = self.gad.query_data(XFMRS_QUERY)
        bindings = results['data']['results']['bindings']
        return bindings


    def PowerTransformerEnd_xfmr_impedances(self):
        VALUES_QUERY = """
        PREFIX r:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX c:  <http://iec.ch/TC57/CIM100#>
        SELECT ?xfmr_name ?from_end ?to_end ?r_ohm ?mesh_x_ohm
        WHERE {
        VALUES ?fdrid {"%s"}
         ?p c:Equipment.EquipmentContainer ?fdr.
         ?fdr c:IdentifiedObject.mRID ?fdrid.
         ?p r:type c:PowerTransformer.
         ?p c:IdentifiedObject.name ?xfmr_name.
         ?from c:PowerTransformerEnd.PowerTransformer ?p.
         ?imp c:TransformerMeshImpedance.FromTransformerEnd ?from.
         ?imp c:TransformerMeshImpedance.ToTransformerEnd ?to.
         ?imp c:TransformerMeshImpedance.r ?r_ohm.
         ?imp c:TransformerMeshImpedance.x ?mesh_x_ohm.
         ?from c:TransformerEnd.endNumber ?from_end.
         ?to c:TransformerEnd.endNumber ?to_end.
        }
        ORDER BY ?xfmr_name ?from_end ?to_end
        """% self.feeder_mrid

        results = self.gad.query_data(VALUES_QUERY)
        bindings = results['data']['results']['bindings']
        return bindings


    def TransformerTank_xfmr_names(self):
        XFMRS_QUERY = """
        PREFIX r:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX c:  <http://iec.ch/TC57/CIM100#>
        SELECT ?xfmr_name ?xfmr_code ?vector_group ?enum ?bus ?baseV ?phase ?grounded ?rground ?xground
        WHERE {
        VALUES ?fdrid {"%s"}
         ?p c:Equipment.EquipmentContainer ?fdr.
         ?fdr c:IdentifiedObject.mRID ?fdrid.
         ?p r:type c:PowerTransformer.
         ?p c:IdentifiedObject.name ?pname.
         ?p c:PowerTransformer.vectorGroup ?vector_group.
         ?t c:TransformerTank.PowerTransformer ?p.
         ?t c:IdentifiedObject.name ?xfmr_name.
         ?asset c:Asset.PowerSystemResources ?t.
         ?asset c:Asset.AssetInfo ?inf.
         ?inf c:IdentifiedObject.name ?xfmr_code.
         ?end c:TransformerTankEnd.TransformerTank ?t.
         ?end c:TransformerTankEnd.phases ?phsraw.
          bind(strafter(str(?phsraw),"PhaseCode.") as ?phase)
         ?end c:TransformerEnd.endNumber ?enum.
         ?end c:TransformerEnd.grounded ?grounded.
         OPTIONAL {?end c:TransformerEnd.rground ?rground.}
         OPTIONAL {?end c:TransformerEnd.xground ?xground.}
         ?end c:TransformerEnd.Terminal ?trm.
         ?trm c:Terminal.ConnectivityNode ?cn.
         ?cn c:IdentifiedObject.name ?bus.
         ?end c:TransformerEnd.BaseVoltage ?bv.
         ?bv c:BaseVoltage.nominalVoltage ?baseV.
        }
        ORDER BY ?xfmr_name ?enum
        """% self.feeder_mrid

        results = self.gad.query_data(XFMRS_QUERY)
        bindings = results['data']['results']['bindings']
        return bindings


    def TransformerTank_xfmr_rated(self):
        VALUES_QUERY = """
        PREFIX r:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX c:  <http://iec.ch/TC57/CIM100#>
        SELECT ?xfmr_name ?xfmr_code ?enum ?ratedS ?ratedU ?connection ?angle ?r_ohm
        WHERE {
        VALUES ?fdrid {"%s"}
         ?eq c:Equipment.EquipmentContainer ?fdr.
         ?fdr c:IdentifiedObject.mRID ?fdrid.
         ?xft c:TransformerTank.PowerTransformer ?eq.
         ?xft c:IdentifiedObject.name ?xfmr_name.
         ?asset c:Asset.PowerSystemResources ?xft.
         ?asset c:Asset.AssetInfo ?t.
         ?p r:type c:PowerTransformerInfo.
         ?t c:TransformerTankInfo.PowerTransformerInfo ?p.
         ?t c:IdentifiedObject.name ?tname.
         ?t c:IdentifiedObject.mRID ?id.
         ?e c:TransformerEndInfo.TransformerTankInfo ?t.
         ?e c:IdentifiedObject.mRID ?eid.
         ?e c:IdentifiedObject.name ?xfmr_code.
         ?e c:TransformerEndInfo.endNumber ?enum.
         ?e c:TransformerEndInfo.ratedS ?ratedS.
         ?e c:TransformerEndInfo.ratedU ?ratedU.
         ?e c:TransformerEndInfo.r ?r_ohm.
         ?e c:TransformerEndInfo.phaseAngleClock ?angle.
         ?e c:TransformerEndInfo.connectionKind ?connraw.
          bind(strafter(str(?connraw),"WindingConnection.") as ?connection)
        }
        ORDER BY ?xfmr_name ?xfmr_code ?enum
        """% self.feeder_mrid

        results = self.gad.query_data(VALUES_QUERY)
        bindings = results['data']['results']['bindings']
        return bindings


    def TransformerTank_xfmr_sct(self):
        VALUES_QUERY = """
        PREFIX r:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX c:  <http://iec.ch/TC57/CIM100#>
        SELECT ?xfmr_name ?enum ?gnum ?leakage_z ?loadloss
        WHERE {
        VALUES ?fdrid {"%s"}
         ?eq c:Equipment.EquipmentContainer ?fdr.
         ?fdr c:IdentifiedObject.mRID ?fdrid.
         ?xft c:TransformerTank.PowerTransformer ?eq.
         ?xft c:IdentifiedObject.name ?xfmr_name.
         ?asset c:Asset.PowerSystemResources ?xft.
         ?asset c:Asset.AssetInfo ?t.
         ?p r:type c:PowerTransformerInfo.
         ?t c:TransformerTankInfo.PowerTransformerInfo ?p.
         ?e c:TransformerEndInfo.TransformerTankInfo ?t.
         ?e c:TransformerEndInfo.endNumber ?enum.
         ?sct c:ShortCircuitTest.EnergisedEnd ?e.
         ?sct c:ShortCircuitTest.leakageImpedance ?leakage_z.
         ?sct c:ShortCircuitTest.loss ?loadloss.
         ?sct c:ShortCircuitTest.GroundedEnds ?grnd.
         ?grnd c:TransformerEndInfo.endNumber ?gnum.
        }
        ORDER BY ?xfmr_name ?enum
        """% self.feeder_mrid

        results = self.gad.query_data(VALUES_QUERY)
        bindings = results['data']['results']['bindings']
        return bindings


    def SwitchingEquipment_switch_names(self):
        SWITCHES_QUERY = """
        PREFIX r:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX c:  <http://iec.ch/TC57/CIM100#>
        SELECT ?sw_name ?base_V ?is_Open ?rated_Current ?breaking_Capacity ?sw_ph_status ?bus1 ?bus2 (group_concat(distinct ?phs1;separator="") as ?phases_side1) (group_concat(distinct ?phs2;separator="") as ?phases_side2)
        WHERE {
        VALUES ?fdrid {"%s"}
         VALUES ?cimraw {c:LoadBreakSwitch c:Recloser c:Breaker c:Fuse c:Sectionaliser c:Jumper c:Disconnector c:GroundDisconnector}
         ?fdr c:IdentifiedObject.mRID ?fdrid.
         ?s r:type ?cimraw.
         bind(strafter(str(?cimraw),"#") as ?cimtype)
         ?s c:Equipment.EquipmentContainer ?fdr.
         ?fdr c:IdentifiedObject.mRID ?fdrid.
         ?s c:IdentifiedObject.name ?sw_name.
         ?s c:ConductingEquipment.BaseVoltage ?bv.
         ?bv c:BaseVoltage.nominalVoltage ?base_V.
         ?s c:Switch.open ?is_Open.
         ?s c:Switch.ratedCurrent ?rated_Current.
         OPTIONAL {?s c:ProtectedSwitch.breakingCapacity ?breaking_Capacity.}
         ?t1 c:Terminal.ConductingEquipment ?s.
         ?t1 c:ACDCTerminal.sequenceNumber "1".
         ?t1 c:Terminal.ConnectivityNode ?cn1.
         ?cn1 c:IdentifiedObject.name ?bus1.
         ?t2 c:Terminal.ConductingEquipment ?s.
         ?t2 c:ACDCTerminal.sequenceNumber "2".
         ?t2 c:Terminal.ConnectivityNode ?cn2.
         ?cn2 c:IdentifiedObject.name ?bus2.
         OPTIONAL {?swp c:SwitchPhase.Switch ?s.
          ?swp c:SwitchPhase.phaseSide1 ?phsraw.
          ?swp c:SwitchPhase.normalOpen ?sw_ph_status.
          bind(strafter(str(?phsraw),"SinglePhaseKind.") as ?phs1)
          ?swp c:SwitchPhase.phaseSide2 ?phsraw2.
          bind(strafter(str(?phsraw2),"SinglePhaseKind.") as ?phs2)}
        }
        GROUP BY ?sw_name ?base_V ?is_Open ?rated_Current ?breaking_Capacity ?sw_ph_status ?bus1 ?bus2
        ORDER BY ?sw_name ?sw_phase_name
        """% self.feeder_mrid

        results = self.gad.query_data(SWITCHES_QUERY)
        bindings = results['data']['results']['bindings']
        return bindings


    def ybus_export(self):
        message = {
        "configurationType": "YBus Export",
        "parameters": {
            "model_id": self.feeder_mrid}
        }

        results = self.gad.get_response("goss.gridappsd.process.request.config", message, timeout=1200)
        return results['data']['yParse'],results['data']['nodeList']


    def vnom_export(self):
        message = {
        "configurationType": "Vnom Export",
        "parameters": {
            "simulation_id": self.simulation_id}
        }

        results = self.gad.get_response("goss.gridappsd.process.request.config", message, timeout=1200)
        return results['data']['vnom']

