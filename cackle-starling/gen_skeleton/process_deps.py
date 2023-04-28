from __future__ import print_function, division
from argparse import ArgumentParser
import json
import sys
import os
import stat
import generate
import common

def gen_invoke(args):
    query_name = "-".join(args.in_file.split(".")[:-1])
    output_file_name = "{}.query_data".format(query_name)
    with open(args.in_file, "r") as in_file, open(output_file_name, "w") as out_data: 
        parsed = json.load(in_file)
        common.update_query_file(parsed, "tmp_params")
        default_partitions = 16
        task_map = {}
        task_map["NUM_NATION"] = 1
        task_map["NUM_REGION"] = 1
        task_map["NUM_CUSTOMER"] = 3
        task_map["NUM_ORDERS"] = 17
        task_map["NUM_LINEITEM"] = 72
        task_map["NUM_SUPPLIER"] = 1
        task_map["NUM_PARTSUPP"] = 12
        task_map["NUM_PART"] = 3
        orig_task_map = task_map.copy()
        for operator in parsed["stages"]:
            if "splits" in operator and operator["type"] in ["BaseTableScanPartition", "BaseTableScanAggregate"]:
                task_map[operator["in_partitions_name"].upper()] = "{}".format(int(task_map[operator["in_partitions_name"].upper()]) * int(operator["splits"]))
            if "num_partitions" in operator:
                part_name = operator["in_partitions_name"].upper()
                task_map[part_name] = int(operator["num_partitions"])
                orig_task_map[part_name] = task_map[part_name]
        in_arguments = generate.get_arguments(parsed["stages"])
        other_arguments = [x.upper() for x in in_arguments if x.upper() not in task_map and x.upper() != "TASKNUM"]
        for argument in other_arguments:
            if argument.find("NUM") >= 0:
                print("INCORRECT BASE TABLE NAMING:", argument)
                sys.exit(1)
            task_map[argument] = default_partitions
            orig_task_map[argument] = task_map[argument]

        for k in task_map:
            out_data.write("{}={}\n".format(k, task_map[k]))
        out_data.write("\n")
        out_data.write("CONSTANTS={}\n".format(" ".join([x.upper() for x in in_arguments[:-1]])))
        out_data.write("\n")
        arguments = generate.get_arguments(parsed["stages"])
        operator_idx_mapping = {}

        index = 0
        operator_name_mapping = {}
        idx_name_map = {}
        for operator in parsed["stages"]:
            #print("{} : {}".format(index, operator["stage_name"]))
            operator_idx_mapping[operator["stage_name"]] = index
            operator_name_mapping[operator["stage_name"]] = operator
            idx_name_map[index] = operator["stage_name"]
            index += 1
        index = 0
        task_count_mapping = {}
        dep_count={}
        for operator in parsed["stages"]:
            tt_dict = {}
            tt_dict["stage_num"] = index
            tt_dict["query_name"] = query_name
            num_tasks = 0
            tasks_name = ""
            parallel = 8
            if operator["type"] == "FinalAggregate":
                num_tasks = 1 if  "in_partitions_name" not in operator else "{}".format(operator["in_partitions_name"].upper())
                tasks_name = str(num_tasks) 
            elif operator["type"] == "Combiner":
                num_tasks = operator["file_factor"]*operator["partition_factor"]
                tasks_name = str(num_tasks)
            elif operator["type"] == "Sleep":
                num_tasks = 1000
                tasks_name = str(num_tasks)
            else:
                tasks_name = operator["in_partitions_name"].upper()
                num_tasks = orig_task_map[tasks_name]
                #if "splits" in operator and operator["type"] in ["BaseTableScanPartition", "BaseTableScanAggregate"]:
                #    tasks_name = str(num_tasks)
                if "split_join" in operator:
                    tasks_name = "{}".format(int(task_map[tasks_name])* int(operator["split_join"][1]))
                else:
                    tasks_name = "{}".format(tasks_name)
            task_count_mapping[index] = tasks_name
            tt_dict["parallel"] = parallel
            tt_dict["tasks_name"] = tasks_name
            skipped_drops = []
            depends_on = []

            if "left_input" in operator and isinstance(operator["left_input"], str):
                depends_on.append(operator_idx_mapping[operator["left_input"]])
                left_op = operator_name_mapping[operator["left_input"]]
                if left_op["type"] in ["FinalAggregate"] or ("drop_left" in operator and operator["drop_left"] == "false"):
                    skipped_drops.append(operator_idx_mapping[operator["left_input"]])
            if "right_input" in operator and isinstance(operator["right_input"], str):
                depends_on.append(operator_idx_mapping[operator["right_input"]])
                right_op = operator_name_mapping[operator["right_input"]]
                if right_op["type"] in ["FinalAggregate"] or ("drop_right" in operator and operator["drop_right"] == "false"):
                    skipped_drops.append(operator_idx_mapping[operator["right_input"]])
            if "in_operator" in operator:
                depends_on.append(operator_idx_mapping[operator["in_operator"]])
                if operator["type"] in ["FinalAggregate"]:
                    skipped_drops.append(operator_idx_mapping[operator["in_operator"]])
            if "broadcast_inputs" in operator:
                for bcast_input in operator["broadcast_inputs"]:
                    depends_on.append(operator_idx_mapping[bcast_input[0]])
                    skipped_drops.append(operator_idx_mapping[bcast_input[0]])

            for dep in depends_on:
                if dep in dep_count:
                    print("Dependency {} found multiple times".format(idx_name_map[dep]))
                else:
                    dep_count[dep] = [operator["stage_name"]]


            tt_dict["depends_on"] = " ".join([str(x) for x in depends_on])
            tt_dict["skipped_drops"] = " ".join([str(x) for x in skipped_drops])

            out_data.write("{stage_num}:{tasks_name}:{depends_on}:{skipped_drops}\n".format(**tt_dict))

            index += 1

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("in_file", type=str)
    args = parser.parse_args()
    gen_invoke(args);
