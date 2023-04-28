import json
def update_query_file(parsed, params_filename):
    with open(params_filename, 'r') as in_file:
        update_query(parsed, in_file.read().split("\n"))

def update_query(parsed, params):
    for param in params:
        if len(param) == 0:
            continue
        param=param.strip()
        splitline = param.split("=")
        keys = splitline[0]
        val = json.loads(splitline[1])
        splitkeys = keys.split(".")
        if splitkeys[0] == "root":
            parsed[splitkeys[1]] = val
        else:
            for op in parsed["stages"]:
                if op["stage_name"][:len(splitkeys[0])] == splitkeys[0]:
                    op[splitkeys[1]] = val

            
