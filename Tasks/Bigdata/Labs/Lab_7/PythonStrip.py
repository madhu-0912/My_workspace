from pig_util import outputSchema
@outputSchemas('value:charray')
def Strip_Function(line):
    return line.strip()