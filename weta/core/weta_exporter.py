import xml.etree.ElementTree as ET
import ast
import pickle
import base64
import json
import string

class Exporter:
    def export(self, ows_file, export_path):
        tree = ET.parse(ows_file)
        schema = tree.getroot()
        node_elements = schema.findall("./nodes/node")
        link_elements = schema.findall("./links/link")
        node_properties_elements = schema.findall("./node_properties/properties")

        self.nodes = {}
        for ne in node_elements:
            node = ne.attrib
            node['inputs'] = {}
            node['outputs'] = {}
            self.nodes[ne.attrib['id']] = node
        # self.nodes = {ne.attrib['id']: ne.attrib for ne in node_elements}
        self.links = [le.attrib for le in link_elements]
        self.node_properties = {np.attrib['node_id']: self.parse_properties(np) for np in node_properties_elements}

        for link in self.links:
            sink_node_id = link['sink_node_id']
            sink_channel = link['sink_channel']
            source_node_id = link['source_node_id']
            source_channel = link['source_channel']

            self.nodes[sink_node_id]['inputs'][sink_channel] = {'node_id': source_node_id, 'channel': source_channel}

            if source_channel not in self.nodes[source_node_id]['outputs']:
                self.nodes[source_node_id]['outputs'][source_channel] = []
            self.nodes[source_node_id]['outputs'][source_channel].append({'node_id': sink_node_id, 'channel': sink_channel})

        for node_id in self.node_properties.keys():
            self.nodes[node_id]['settings'] = self.node_properties[node_id]['settings']
            self.nodes[node_id]['settings_format'] = self.node_properties[node_id]['settings_format']

        roots = []
        for node in self.nodes.values():
            if len(node['inputs'].keys()) == 0:
                roots.append(node)
        # roots = list(filter(lambda node: len(node['inputs'].keys()) == 0, self.nodes))

        for root in roots:

            stack = [root]
            variables = set()
            # variables.add('inputs%s' % root['id'])
            # code = 'inputs%s = None \n' % root['id']
            code = ''
            while len(stack) > 0:
                node = stack.pop()

                unsatisfied_input = False
                for input in node['inputs'].keys():
                    input_node_id = node['inputs'][input]['node_id']
                    if 'outputs' + input_node_id not in variables:
                        unsatisfied_input = True
                        stack.append(node)
                        stack.append(self.nodes[input_node_id])
                        break

                if unsatisfied_input:
                    continue

                # start generating
                code += '\n\n# %s \n' % node['title']
                code += 'inputs%s = {' % node['id']
                for input in node['inputs'].keys():
                    input_node_id = node['inputs'][input]['node_id']
                    input_node_channel = node['inputs'][input]['channel']
                    code += '"%s": outputs%s["%s"]' % (input, input_node_id, input_node_channel)
                code += '} \n'
                qualified_name: str = node['qualified_name']
                func_name = qualified_name.split('.')[-2]

                # for key in list(node['settings'].keys()):
                #     if isinstance(node['settings'][key], bytes):
                #         node['settings'].pop(key)

                code += 'settings%s = eval_settings("%s", "%s") \n' % (node['id'], node['settings'], node['settings_format'])
                code += 'outputs%s = %s(inputs, settings) \n' % (node['id'], func_name)
                variables.add('outputs%s' % node['id'])

                for output in node['outputs'].keys():
                    output_nodes = node['outputs'][output]
                    for output_node in output_nodes:
                        output_node_id = output_node['node_id']
                        stack.append(self.nodes[output_node_id])

            print(code)


    def generate(self, node, inputs, settings):
        qualified_name: str = node['qualified_name']
        func_name = qualified_name.split('.')[-2]

        code = 'settings = %s \n' % json.dump(node['settings'])

        code += 'outputs = %s(inputs, settings) \n' % func_name

        # code = 'inputs = None \n'
        # code += 'settings = %s' % json.dump(node['settings'])
        # while node['outputs'] is not None:
        #     qualified_name: str = node['qualified_name']
        #     func_name = qualified_name.split('.')[-2]
        #     code += 'outputs = %s(inputs, settings)' % func_name
        return code


    @staticmethod
    def parse_properties(np):
        format = np.get('format')
        node_id = np.get('node_id')
        data: str = np.text
        import re
        data = re.sub(r"(, )?'savedWidgetGeometry': b'\S+'", '', data)
        data = re.sub(r"(, )?'__version__': \d+", '', data)

        return {'node_id': node_id, 'settings': data, 'settings_format': format}


if __name__ == '__main__':
    exporter = Exporter()
    exporter.export('/Users/Chao/nzpolice/summer/weta.ows', '')