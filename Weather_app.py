import re
def parse_temperature(temp_str: str):
    temp = re.findall(r'\d+', temp_str)  # pattern first, string second
    return int(temp[0]) if temp else None  # get first match from list
def parse_humidity(humid_str: str):
    return int(humid_str)
