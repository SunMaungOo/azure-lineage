from model import Parameter,ParameterType
from typing import Union,Dict

def create_parameter(parameter_value:Union[str,Dict[str,str]])->Parameter:
    if isinstance(parameter_value,str):
        return Parameter(value=parameter_value,parameter_type=ParameterType.Static)

    return Parameter(value=parameter_value["value"],parameter_type=ParameterType.Expression)