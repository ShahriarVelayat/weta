import xml.etree.ElementTree as ET
import ast
import pickle
import base64

class Exporter:
    def export(self, ows_file, export_path):
        tree = ET.parse(ows_file)
        schema = tree.getroot()
        node_elements = schema.findall("./nodes/node")
        link_elements = schema.findall("./links/link")
        node_properties_elements = schema.findall("./node_properties/properties")

        self.nodes = {ne.attrib['id']: ne.attrib for ne in node_elements}
        self.links = [le.attrib for le in link_elements]
        self.node_properties = {np.attrib['node_id']: self.parse_properties(np) for np in node_properties_elements}

        for link in self.links:
            sink_node_id = link['sink_node_id']
            sink_channel = link['sink_channel']
            if 'inputs' not in self.nodes[sink_node_id]:
                self.nodes[sink_node_id]['inputs'] = []
            self.nodes[sink_node_id]['inputs'].append(sink_channel)

            source_node_id = link['source_node_id']
            source_channel = link['source_channel']
            if 'outputs' not in self.nodes[source_node_id]:
                self.nodes[source_node_id]['outputs'] = []
            self.nodes[source_node_id]['outputs'].append(source_channel)

        for node_id in self.node_properties.keys():
            self.nodes[node_id]['properties'] = self.node_properties[node_id]['settings']


        roots = filter(lambda node: node['inputs'].empty(), self.nodes)

        for root in roots:
            pass

    def generate(self, node, inputs, settings):
        qualified_name: str = node['qualified_name']
        func_name = qualified_name.split('.')[-2]

        ret = 'ret = %s(inputs, settings)'


        return


    @staticmethod
    def parse_properties(np):
        format = np.get('format')
        node_id = np.get('node_id')
        data = np.text
        properties = None
        if format == 'literal':
            properties = ast.literal_eval(data)
        if format == 'pickle':
            properties = pickle.loads(base64.decodebytes(data.encode('ascii')))
        return {'node_id': node_id, 'settings': properties}


if __name__ == '__main__':
    exporter = Exporter()
    exporter.export('/Users/Chao/summer/weta.ows', '')