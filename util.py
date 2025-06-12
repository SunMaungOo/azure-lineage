from model import Parameter,ParameterType
from typing import Union,Dict

def create_parameter(parameter_value:Union[str,Dict[str,str]])->Parameter:
    if isinstance(parameter_value,str):
        return Parameter(value=parameter_value,parameter_type=ParameterType.Static)

    return Parameter(value=parameter_value["value"],parameter_type=ParameterType.Expression)

def get_connection_properties(connection_str:str)->Dict[str,str]:
    connection_propertity:Dict[str,str] = dict()

    for block in connection_str.split(";"):
        property_block = block.split("=")

        if len(property_block)>=2:
            connection_propertity[property_block[0].replace(" ","").lower()] = property_block[1]

    return connection_propertity

