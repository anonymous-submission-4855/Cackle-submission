from __future__ import print_function, division
from argparse import ArgumentParser
import json
import sys
import re
import common

def gen_selected_columns(operator):
    output = ""
    alias_len = 0
    if "alias" in operator:
        alias_len = len(operator["alias"]) + 1
    for col in operator["in_columns"]:#each selected column:
        if col[1] == "CUSTOM":
            continue
        output += "  selectedColumns.push_back(std::string(\"{}\"));\n".format(col[0][alias_len:]);
    return output

def gen_vector_batch(in_columns):
    output = ""
    idx = 0
    for col in in_columns:#each selected column:
        orcType = ""
        strLen = -1
        if col[1] == "LONG":
            orcType = "Long"
        elif col[1] == "DATE":
            orcType = "Long"
        elif col[1] == "DOUBLE":
            orcType = "Double"
        elif col[1] == "STRING":
            orcType = "String"
            strLen = int(col[2])
        else:
            continue
        output += "  orc::{orcType}VectorBatch * {col_name}_vec = (orc::{orcType}VectorBatch *)((orc::StructVectorBatch*) batch.get())->fields.at({idx});\n".format(orcType=orcType, col_name = col[0], idx=idx)
        idx+=1
    return output

def gen_row_data(in_columns):
    output = ""
    for col in in_columns:
        if col[1] == "CUSTOM":
            output += col[2].format(col_name = col[0])
        if col[1] == "LONG":
            output += "      const int64_t &{col_name} = {col_name}_vec->data[i];\n".format(col_name=col[0]);
        elif col[1] == "DOUBLE":
            output += "      const double &{col_name} = {col_name}_vec->data[i];\n".format(col_name=col[0]);
        elif col[1] == "DATE":
            output += "      const time_t {col_name} = {col_name}_vec->data[i] * DATE_MULTIPLIER;\n".format(col_name=col[0])
        elif col[1] == "STRING":
            output += "      const std::string {col_name}({col_name}_vec->data[i], {col_name}_vec->length[i]);\n".format(col_name=col[0])
    return output


def generate_scan_record(operator):
    output = " "*8
    output += "int partitionNum = ({partition_column} % 91397) % outPartitions;\n".format(partition_column=operator["partition_column"])
    output += "struct {}_record out_rec;\n".format(get_request_name(operator).lower())
    #TODO handle dictionary encoding
    has_dict = False
    for column in operator["out_columns"]:
        if column[1] == "STRING" and len(column) >= 4 and column[3] == "DICT":
            has_dict = True
    if has_dict:
        output += " "*8+"muxs[0].lock();\n"
    for column in operator["out_columns"]:
        if len(column) >= 3 and column[2] == "CUSTOM":
            output += column[3]
        elif len(column) >= 4 and column[3] == "CUSTOM":
            output += column[4]
        elif column[1] == "LONG" or column[1] == "DATE" or column[1] == "DOUBLE":
            output += " "*8+ "out_rec.{} = {};\n".format(column[0], column[0])
        elif column[1] == "STRING":
            if len(column) >= 4 and column[3] == "DICT":
                output += " "*8 + "if ({}_dict_encoding_write.find({}) == {}_dict_encoding_write.end()){{\n".format(column[0], column[0], column[0])
                output += " "*10 + "{}_dict_encoding_write[{}] = {}_curr_encoding++;\n".format(column[0], column[0], column[0])
                output += " "*8 + "}"
                output += " "*8 + "out_rec.{}_enc = {}_dict_encoding_write[{}];\n".format(column[0], column[0], column[0])
            else:
                output += " "*8+ "std::memcpy(out_rec.{}, {}_vec->data[i], {}_vec->length[i]);\n".format(column[0], column[0], column[0]);
                output += " "*8+ "if ({0}_vec->length[i] < {1}) std::memset(out_rec.{0}+{0}_vec->length[i], ' ', {1}-{0}_vec->length[i]);\n".format(column[0], column[2]);
    if has_dict:
        output += " "*8+"muxs[0].unlock();\n"
    return output

def gen_scan_row_processing(operator):
    output = ""
    output += generate_scan_record(operator)
    output += " "*8+"muxs[partitionNum%NUM_MUXS].lock();\n"
    output += write_scan_record(operator)
    output += " "*8+"muxs[partitionNum%NUM_MUXS].unlock();\n"
    return output


def write_scan_record(operator):
    output = " "*8+"partitionData->at(partitionNum).numRecords++;\n"
    output += " "*8+"partitionData->at(partitionNum).buf.write(reinterpret_cast<char *>(&out_rec), {}_RECORD_SIZE);\n".format(get_request_name(operator))
    return output

def generate_dictionary_encodings_header(operators):
    needed_dicts = {}
    for operator in operators:
        if "out_columns" not in operator and operator["type"] != "TwoBaseTableJoinPartition":
            continue
        if operator["type"] == "TwoBaseTableJoinPartition":
            for column in operator["left_input"]["out_columns"]:
                if len(column) >= 4 and column[3] == "DICT":
                    needed_dicts[column[0]] = column

        for column in operator["out_columns"]:
            if len(column) >= 4 and column[3] == "DICT":
                needed_dicts[column[0]] = column
    output = ""
    for name in needed_dicts:
        column = needed_dicts[name]
        output += " "*4+"uint8_t {}_curr_encoding = 0;\n\n".format(name)
        output += " "*4+"std::unordered_map<std::string, uint8_t> {}_dict_encoding_write ;\n".format(name)
        output += " "*4+"std::unordered_map<uint8_t, std::string> {}_dict_encoding_read ;\n".format(name)
    #generate structs
    for name in needed_dicts:
        column = needed_dicts[name]
        output += " "*4+"struct {}_dict_record{{\n".format(name) 
        output += " "*4+"  uint8_t enc;\n"
        output += " "*4+"  char {}[{}];\n".format(name, column[2])
        output += " "*4+"};\n"
        output += " "*4+"int {}_DICT_RECORD_SIZE = {};\n\n".format(name.upper(), int(column[2])+1)
    return output


def gen_constants(constants):
    header_def = ""
    cpp_def = ""
    for constant in constants:
        header_def += "    {} {};\n".format(constant["type"], constant["name"])
        cpp_def += "  {}\n".format(constant["initialization"])

    return header_def, cpp_def

def get_request_name_from_name(stage_name):
    return "_".join(re.findall("[a-zA-Z][^A-Z]*", stage_name)).upper()

def get_request_name(operator):
    return get_request_name_from_name(operator["stage_name"])


def get_request_names(operators):
    output = []
    for operator in operators:
        output.append(get_request_name(operator))
    return output

def gen_request_switch(operators, operator_mapping):
    output = ""

    for operator in operators:
        output += " "*4+"case {}:\n".format(get_request_name(operator))
        left_multiplier = 1
        right_multiplier = 1
        in_multiplier = 1
        if operator["type"]  in ["PartitionedHashJoin", "PartitionedHashJoinAggregate"]:
            if "split_join" in operator_mapping[operator["left_input"]]:
                left_multiplier = operator_mapping[operator["left_input"]]["split_join"][1]
            if "split_join" in operator_mapping[operator["right_input"]]:
                right_multiplier = operator_mapping[operator["right_input"]]["split_join"][1]

        if operator["type"] == "BaseTableScanAggregate":
            output += " "*6+"{}({}, taskNum);\n".format(operator["stage_name"], 1 if "out_partitions_name" not in operator else operator["out_partitions_name"])
        elif operator["type"] == "Sleep":
            output += " "*6+"{}({});\n".format(operator["stage_name"], operator["time"])
        elif operator['type'] == "BaseTableScanPartition" or operator["type"] == "BroadcastJoinBaseTable" or operator["type"] == "TwoBaseTableJoinPartition":
            output += " "*6+"{}({}, taskNum);\n".format(operator["stage_name"], operator["out_partitions_name"])
        elif operator['type'] == "PartitionedHashJoin":
            left_operator = operator_mapping[operator["left_input"]]
            right_operator = operator_mapping[operator["right_input"]]
            numRightFiles = 0 
            numLeftFiles = 0
            if right_operator["type"] == "Combiner":
                numRightFiles = right_operator["file_factor"]*right_operator["partition_factor"]
            else:
                numRightFiles = right_operator["in_partitions_name"]

            if left_operator["type"] == "Combiner":
                numLeftFiles = left_operator["file_factor"]*left_operator["partition_factor"]
            else:
                numLeftFiles = left_operator["in_partitions_name"]
            output += " "*6+"{}({}*{}, {}*{}, {}, {}, taskNum);\n".format(operator["stage_name"], numLeftFiles,left_multiplier, numRightFiles, right_multiplier, operator["in_partitions_name"], operator["out_partitions_name"])
        elif operator['type'] == "PartitionedHashJoinAggregate":
            left_operator = operator_mapping[operator["left_input"]]
            right_operator = operator_mapping[operator["right_input"]]
            numRightFiles = 0 
            numLeftFiles = 0
            if right_operator["type"] == "Combiner":
                numRightFiles = right_operator["file_factor"]*right_operator["partition_factor"]
            else:
                numRightFiles = right_operator["in_partitions_name"]

            if left_operator["type"] == "Combiner":
                numLeftFiles = left_operator["file_factor"]*left_operator["partition_factor"]
            else:
                numLeftFiles = left_operator["in_partitions_name"]

            output += " "*6+"{}({}*{}, {}*{}, {}, {} taskNum);\n".format(operator["stage_name"], numLeftFiles, left_multiplier , numRightFiles, right_multiplier, operator["in_partitions_name"], "" if "out_partitions_name" not in operator else operator["out_partitions_name"]+",")
        elif operator['type'] == "FinalAggregate":
            if "split_join" in operator_mapping[operator["in_operator"]]:
                in_multiplier = operator_mapping[operator["in_operator"]]["split_join"][1]
            in_operator = operator_mapping[operator["in_operator"]]
            output += " "*6+"{}({}*{}, {}, taskNum);\n".format(operator["stage_name"], in_operator["out_partitions_name"] if "out_partitions_name" in in_operator else in_operator["in_partitions_name"], in_multiplier, 1 if "in_partitions_name" not in operator else operator["in_partitions_name"])
        elif operator['type'] == "Combiner":
            in_operator = operator_mapping[operator["in_operator"]]
            output += " "*6+"{}({}, {}, taskNum);\n".format(operator["stage_name"], in_operator["in_partitions_name"], in_operator["out_partitions_name"])
        else: 
            print("unknown operator type: {}".format(operator["type"]))
            sys.exit(1)
        output += " "*6+"break;\n"
    return output

def gen_hash_functions(class_name, operators):
    output = ""
    for operator in operators:
        if operator["type"] != "PartitionedHashJoinAggregate" and operator["type"] != "BaseTableScanAggregate":
            continue
        output += " "*2+"template <>\n"
        output += " "*2+"struct hash<starling::{}::{}_group_by_record>{{\n".format(class_name, get_request_name(operator).lower())
        output += " "*4+"std::size_t operator()(const starling::{}::{}_group_by_record& k) const{{\n".format(class_name, get_request_name(operator).lower())
        output += " "*6+"return "
        for column in operator["group_by_columns"]:
            if (column[1] == "LONG" or column[1] == "DATE" or column[1] == "DOUBLE"):
                output += "hash<int64_t>()(k.{}) ^".format(column[0])
            elif(column[1] == "STRING"):
                output += "hash<string>()(std::string(k.{}, {})) ^".format(column[0], column[2])

        output += "0;\n"
        output += " "*4+"}\n"
        output += " "*2+"};\n\n"
    return output

def join_build_phase_process_record(operator, operator_mapping, left_rec_name="left_rec", exists=""):
    left_operator = ""
    if isinstance(operator["left_input"], str):
        left_operator = operator_mapping[operator["left_input"]]
    else:
        left_operator = operator["left_input"]
    if "type" in left_operator and left_operator["type"] == "Combiner":
        left_operator =  operator_mapping[left_operator["in_operator"]]
    tt_dict = {}
    tt_dict["left_join_key"]=left_operator["partition_column"]
    tt_dict["left_rec_name"]=left_rec_name
    template = " "*6+"muxs[0].lock();\n"
    if exists == "EXISTS" or exists == "NOT EXISTS" or exists.find("left") >= 0:
        template += " "*6 + "if ((*hash_map).find({left_rec_name}.{left_join_key}) == (*hash_map).end()) \n"
        template += " "*8+"(*hash_map)[{left_rec_name}.{left_join_key}].push_back({left_rec_name});\n"
 
    else:
        template += " "*6+"(*hash_map)[{left_rec_name}.{left_join_key}].push_back({left_rec_name});\n"
    template += " "*6+"muxs[0].unlock();\n"
    output = template.format(**tt_dict)
    return output

def bcast_build_phase_process_record(bcast_operator, bcast_idx, exists=""):
    tt_dict = {}
    tt_dict["join_key"]=bcast_operator["partition_column"]
    tt_dict["left_rec_name"]="left_rec"
    tt_dict["bcast_idx"]=bcast_idx
    template = " "*6+"muxs[0].lock();\n"
    template += " "*6+"(*bcast_hash_map{bcast_idx})[{left_rec_name}.{join_key}].push_back({left_rec_name});\n"
    template += " "*6+"muxs[0].unlock();\n"
    output = template.format(**tt_dict)
    return output

def partition_join_probe_phase_process_record(operator, operator_mapping, left_rec_name="left_rec.", right_rec_name="right_rec."):
    left_operator = ""
    right_operator = ""
    if isinstance(operator["left_input"], str):
        left_operator = operator_mapping[operator["left_input"]]
        if left_operator["type"] == "Combiner":
            left_operator =  operator_mapping[left_operator["in_operator"]]
    else:
        left_operator = operator["left_input"]
    if isinstance(operator["right_input"], str):
        right_operator = operator_mapping[operator["right_input"]]
        if right_operator["type"] == "Combiner":
            right_operator =  operator_mapping[right_operator["in_operator"]]
    else:
        right_operator = operator["right_input"]
    tt_dict = {}
    tt_dict["left_record_type"]=get_record_type(left_operator)
    tt_dict["right_join_key"]=right_operator["partition_column"]
    tt_dict["filter_condition"]=operator["filter_condition"]
    tt_dict["out_partition_key"]=operator["partition_column"]
    tt_dict["create_output_record"]=gen_output_record(operator, right_rec_name)
    tt_dict["out_record_size"]=get_record_size(operator)
    tt_dict["right_rec_name"]=right_rec_name
    exists = ""
    if exists in operator:
        exists = operator["exists"]
    if exists.find("left") >= 0:
        template = """
        auto it = hash_map->find(right_rec.{right_join_key};
        if (it {comparator} hash_map->end()){{
            if ({filter_condition}){{
      {create_output_record}
              int partitionNum = (out_rec.{out_partition_key} % 91397) % outPartitions;
              muxs[partitionNum%NUM_MUXS].lock();
              partitionData->at(partitionNum).numRecords++;
              partitionData->at(partitionNum).buf.write(reinterpret_cast<char*>(&out_rec), {out_record_size});
              muxs[partitionNum%NUM_MUXS].unlock();
            }}
        }}
        """
        # if it exists lookup suceeds
        # //deprecated for the moment
        if exists.find("NOT EXISTS") < 0:
            tt_dict["comparator"] = "!="
        else:
            tt_dict["comparator"] = "=="
    elif "exists_cond" in operator and operator["exists_cond"] == "exists left":
        template = """
        for (struct {left_record_type} &left_rec : (*hash_map)[{right_rec_name}{right_join_key}]){{
            if ({filter_condition}){{
      {create_output_record}
              int partitionNum = (out_rec.{out_partition_key} % 91397) % outPartitions;
              muxs[partitionNum%NUM_MUXS].lock();
              partitionData->at(partitionNum).numRecords++;
              partitionData->at(partitionNum).buf.write(reinterpret_cast<char*>(&out_rec), {out_record_size});
              muxs[partitionNum%NUM_MUXS].unlock();
              break;
            }}
        }}
        """
    elif "exists_cond" in operator and operator["exists_cond"] == "not exists left":
        template = """
        bool record_exists = false;
        for (struct {left_record_type} &left_rec : (*hash_map)[{right_rec_name}{right_join_key}]){{
            if ({filter_condition}){{
              record_exists = true;
              break;
            }}
        }}
        if (!record_exists) {{
          {create_output_record}
          int partitionNum = (out_rec.{out_partition_key} % 91397) % outPartitions;
          muxs[partitionNum%NUM_MUXS].lock();
          partitionData->at(partitionNum).numRecords++;
          partitionData->at(partitionNum).buf.write(reinterpret_cast<char*>(&out_rec), {out_record_size});
          muxs[partitionNum%NUM_MUXS].unlock();
        }}
        """
    else:
        template = """
        if (hash_map->find({right_rec_name}{right_join_key}) != hash_map->end()){{
          for (struct {left_record_type} &left_rec : (*hash_map)[{right_rec_name}{right_join_key}]){{
            if ({filter_condition}){{
      {create_output_record}
              int partitionNum = (out_rec.{out_partition_key} % 91397) % outPartitions;
              muxs[partitionNum%NUM_MUXS].lock();
              partitionData->at(partitionNum).numRecords++;
              partitionData->at(partitionNum).buf.write(reinterpret_cast<char*>(&out_rec), {out_record_size});
              muxs[partitionNum%NUM_MUXS].unlock();
            }}
          }}
        }}
        """
    output = template.format(**tt_dict)
    return output

def create_bcast_probes(operator, operator_mapping):
    index = len(operator["broadcast_inputs"])
    output = """
{create_aggregate_record}
    """.format(create_aggregate_record=create_agg_record(operator, operator_mapping))
    for bcast_input in operator["broadcast_inputs"][::-1]:
        curr_template = """
    if (bcast_hash_map{index}->find({probe_key}) != bcast_hash_map{index}->end()){{
        for (struct {bcast_record_type} &bcast_rec{index} : (*bcast_hash_map{index})[{probe_key}]){{
          if ({filter_condition}){{
          {prev_output}
          }}
        }}
      }}
        """
        output = curr_template.format(index=index, bcast_record_type=get_record_type(operator_mapping[bcast_input[0]]), probe_key=bcast_input[1], filter_condition=bcast_input[2], prev_output=output)
        index -= 1

    return output

def agg_join_probe_bcast_process_record(operator, operator_mapping, exists =""):
    left_operator = operator_mapping[operator["left_input"]]
    right_operator = operator_mapping[operator["right_input"]]
    if left_operator["type"] == "Combiner":
        left_operator =  operator_mapping[left_operator["in_operator"]]
    if right_operator["type"] == "Combiner":
        right_operator =  operator_mapping[right_operator["in_operator"]]
    tt_dict = {}
    tt_dict["left_record_type"]=get_record_type(left_operator)
    tt_dict["right_join_key"]=right_operator["partition_column"]
    tt_dict["filter_condition"]=operator["filter_condition"]
    tt_dict["create_aggregate_record"] = create_bcast_probes(operator, operator_mapping)
    template = """
    if (hash_map->find(right_rec.{right_join_key}) != hash_map->end()){{
        for (struct {left_record_type} &left_rec : (*hash_map)[right_rec.{right_join_key}]){{
          if ({filter_condition}){{
    {create_aggregate_record}
          }}
        }}
      }}
    """
    if exists.find("left") >= 0:
        template = """
        auto it = hash_map->find(right_rec.{right_join_key});
        if (it {comparator} hash_map->end()){{
          if ({filter_condition}){{
    {create_aggregate_record}
            }}
          }}
        """
        # if it exists lookup suceeds
        # //deprecated for the moment
        if exists.find("NOT EXISTS") < 0:
            tt_dict["comparator"] = "!="
        else:
            tt_dict["comparator"] = "=="
    else:
        template = """
        auto it = hash_map->find(right_rec.{right_join_key});
        if (it != hash_map->end()){{
            for (struct {left_record_type} &left_rec : it->second){{
              if ({filter_condition}){{
    {create_aggregate_record}
              }}
            }}
          }}
        """
    if "outer" in operator:
        tt_dict["create_aggregate_record_outer"]=create_agg_record_outer(operator, operator_mapping)
        template += """
        else{{
    {create_aggregate_record_outer}
          }}
        """
    return template.format(**tt_dict)

def create_agg_record_outer(operator, operator_mapping):
    output = create_agg_record(operator, operator_mapping)
    if operator["outer"]:
        output= "\n".join(output.split("\n")[:-2])
    return output


def aggregate_join_probe_phase_process_record(operator, operator_mapping, exists=""):
    left_operator = operator_mapping[operator["left_input"]]
    right_operator = operator_mapping[operator["right_input"]]
    if left_operator["type"] == "Combiner":
        left_operator =  operator_mapping[left_operator["in_operator"]]
    if right_operator["type"] == "Combiner":
        right_operator =  operator_mapping[right_operator["in_operator"]]
    tt_dict = {}
    tt_dict["left_record_type"]=get_record_type(left_operator)
    tt_dict["right_join_key"]=right_operator["partition_column"]
    tt_dict["filter_condition"]=operator["filter_condition"]
    tt_dict["create_aggregate_record"]=create_agg_record(operator, operator_mapping)
    template = ""
    if exists.find("left") >= 0:
        template = """
        auto it = hash_map->find(right_rec.{right_join_key});
        if (it {comparator} hash_map->end()){{
          if ({filter_condition}){{
    {create_aggregate_record}
            }}
          }}
        """
        # if it exists lookup suceeds
        # //deprecated for the moment
        if exists.find("NOT EXISTS") < 0:
            tt_dict["comparator"] = "!="
        else:
            tt_dict["comparator"] = "=="
    else:
        template = """
        auto it = hash_map->find(right_rec.{right_join_key});
        if (it != hash_map->end()){{
            for (struct {left_record_type} &left_rec : it->second){{
              if ({filter_condition}){{
    {create_aggregate_record}
              }}
            }}
          }}
        """
    if "outer" in operator:
        tt_dict["create_aggregate_record_outer"]=create_agg_record_outer(operator, operator_mapping)
        template += """
        else{{
    {create_aggregate_record_outer}
          }}
        """

    return template.format(**tt_dict)

def gen_struct_for_filter(operator):
    output = ""
    output += "    struct {}_record{{\n".format(get_request_name(operator).lower())
    total_size = 0
    for column in operator["out_columns"]:
        currline = " "*6
        if (column[1] == "LONG" or column[1] == "DATE"):
            currline += "int64_t {};\n".format(column[0]);
            total_size += 8;
        elif (column[1] == "DOUBLE"):
            currline += "double {};\n".format(column[0]);
            total_size += 8;
        elif(column[1] == "STRING"):
            if (len(column) >= 4 and column[3] == "DICT"):
                currline += "uint8_t {}_enc;\n".format(column[0]); 
                total_size += 1
            else: 
                currline += "char {}[{}];\n".format(column[0], column[2]); 
                total_size += int(column[2]);
        output += currline
    output += "    };\n\n";
    output += "    int {}_RECORD_SIZE = {};\n\n".format(get_request_name(operator), total_size);
    return output


def gen_structs(operators):
    output = ""
    for operator in operators:
        if operator["type"] == "PartitionedHashJoinAggregate" or operator["type"] == "BaseTableScanAggregate":
            total_size = 0
            output += "  public:\n"
            output += "    struct {}_record{{\n".format(get_request_name(operator).lower())
            for column in operator["aggregate_columns"]:
                currline = " "*6
                if (column[1] == "LONG"):
                    currline += "int64_t {};\n".format(column[0]);
                    total_size += 8;
                elif(column[1] == "DOUBLE"):
                    currline += "double {};\n".format(column[0]); 
                    total_size += 8;
                output += currline
            for column in operator["group_by_columns"]:
                currline = " "*6
                if (column[1] == "LONG" or column[1] == "DATE"):
                    currline += "int64_t {};\n".format(column[0]);
                    total_size += 8;
                if (column[1] == "DOUBLE"):
                    currline += "double {};\n".format(column[0]);
                    total_size += 8;
                elif(column[1] == "STRING"):
                    currline += "char {}[{}];\n".format(column[0], column[2]); 
                    total_size += int(column[2]);
                output += currline
            if "order_by_columns" in operator:
                output += " "*6+"static bool compareTwo(const {0}_record &a, const {0}_record &b){{\n".format(get_request_name(operator).lower())
                return_output = " "*8+"return {};\n"
                for column in operator["order_by_columns"]:
                    if (column[1] == "LONG" or column[1] == "DATE" or column[1] == "DOUBLE"):
                        return_output = return_output.format("a.{0} {first_cmp} b.{0} ? true : (a.{0} {second_cmp} b.{0} ? false : {{}}) ".format(column[0], first_cmp="<" if column[-1] != "DESC" else ">", second_cmp=">" if column[-1] != "DESC" else "<"))
                    elif(column[1] == "STRING"):
                        return_output = return_output.format("strncmp(a.{0}, b.{0}, {length}) {first_cmp} 0 ? true : (strncmp(a.{0}, b.{0}, {length}) {second_cmp} 0 ? false : {{}}) ".format(column[0], length=column[2],first_cmp="<" if column[-1] != "DESC" else ">", second_cmp=">" if column[-1] != "DESC" else "<"))
                output += return_output.format("false");
                output += " "*6+"}\n"
            output += "    };\n\n";

            output += "    int {}_RECORD_SIZE = {};\n\n".format(get_request_name(operator), total_size);

            output += "    struct {}_group_by_record{{\n".format(get_request_name(operator).lower())
            total_size = 0
            for column in operator["group_by_columns"]:
                currline = " "*6
                if (column[1] == "LONG" or column[1] == "DATE"):
                    currline += "int64_t {};\n".format(column[0]);
                    total_size += 8;
                elif (column[1] == "DOUBLE"):
                    currline += "double {};\n".format(column[0]);
                    total_size += 8;
                elif(column[1] == "STRING"):
                    currline += "char {}[{}];\n".format(column[0], column[2]); 
                    total_size += int(column[2]);
                output += currline

            output += " "*6+"bool operator==(const {}_group_by_record &other) const{{\n".format(get_request_name(operator).lower())
            output += " "*8+"return "
            for column in operator["group_by_columns"]:
                if (column[1] == "LONG" or column[1] == "DATE" or column[1] == "DOUBLE"):
                    output += "{} == other.{} && ".format(column[0], column[0])
                elif(column[1] == "STRING"):
                    output += "strncmp({}, other.{}, {}) == 0 && ".format(column[0], column[0], column[2])
            output += " true;\n"
            output += " "*6+"}\n"

            output += " "*6+"bool operator<(const {}_group_by_record &other) const{{\n".format(get_request_name(operator).lower())
            return_output = " "*8+"return {};\n"
            for column in operator["group_by_columns"]:
                if (column[1] == "LONG" or column[1] == "DATE" or column[1] == "DOUBLE"):
                    return_output = return_output.format("{} < other.{} ? true : ({} > other.{} ? false : {{}}) ".format(column[0], column[0], column[0], column[0]))
                elif(column[1] == "STRING"):
                    return_output = return_output.format("strncmp({}, other.{}, {}) < 0 ? true : (strncmp({}, other.{}, {}) > 0 ? false : {{}}) ".format(column[0], column[0], column[2], column[0], column[0], column[2]))
            output += return_output.format("false");
            output += " "*6+"}\n"
            output += "    };\n\n";

            output += "    int {}_GROUP_BY_RECORD_SIZE = {};\n\n".format(get_request_name(operator), total_size);
            output += "  private:\n"
            output += "    struct {}_agg_record{{\n".format(get_request_name(operator).lower())
            total_size = 0
            for column in operator["aggregate_columns"]:
                currline = " "*6
                if (column[1] == "LONG"):
                    currline += "int64_t {};\n".format(column[0]);
                    total_size += 8;
                elif(column[1] == "DOUBLE"):
                    currline += "double {};\n".format(column[0]); 
                    total_size += 8;
                output += currline
            output += "    };\n\n";
            output += " "*4+"int {}_AGG_RECORD_SIZE = {};\n\n".format(get_request_name(operator), total_size);
        elif operator["type"] in  ["FinalAggregate", "Combiner", "Sleep"] :
            continue
        elif operator["type"] == "TwoBaseTableJoinPartition":
            output += gen_struct_for_filter(operator["left_input"])
            output += gen_struct_for_filter(operator)
        else:
            output += gen_struct_for_filter(operator)
    return output



def gen_request_types(operators):
    request_types = get_request_names(operators)
    request_types[0] = request_types[0]+" = 0";
    for i in range(len(request_types)):
        request_types[i] = "      "+request_types[i]
    output = ",\n".join(request_types)
    return output

def get_arguments(operators):
    arguments = []
    for operator in operators:
        if "in_partitions_name" in operator and operator["in_partitions_name"] not in arguments:
            arguments.append(operator["in_partitions_name"])
        if "out_partitions_name" in operator and operator["out_partitions_name"] not in arguments:
            arguments.append(operator["out_partitions_name"])
    arguments.append("taskNum")
    return arguments

def gen_output_dictionaries(operator):
    output = ""
    for column in operator["out_columns"]:
        if len(column) >= 4 and column[3] == "DICT":
            output +=  " "*2+"outs->write(reinterpret_cast<char*>(&{}_curr_encoding), sizeof({}_curr_encoding));\n".format(column[0], column[0])
            output +=  " "*2+"struct {}_dict_record {}_enc_rec;\n".format(column[0], column[0])
            output += " "*2+"for (auto it = {}_dict_encoding_write.begin(); it != {}_dict_encoding_write.end(); ++it){{\n".format(column[0], column[0])
            output += " "*4+"{}_enc_rec = {{}};\n".format(column[0])
            output += " "*4+"{}_enc_rec.enc = it->second;\n".format(column[0])
            output += " "*4+"std::memcpy({}_enc_rec.{}, it->first.data(), it->first.size());\n".format(column[0], column[0], column[0], column[0])
            output += " "*4+"outs->write(reinterpret_cast<char*>(&{}_enc_rec), {}_DICT_RECORD_SIZE);\n".format(column[0], column[0].upper())
            output += " "*2+"};\n"
    return output



def gen_parse_args(operators):
    arguments = get_arguments(operators)
    output = "  if (argc < {}) {{\n".format(len(arguments)+3)
    output += "    std::cout << \"Usage: \" << argv[0] << \" request_type query_id {}\" << std::endl;\n".format(" ".join(arguments))
    output += "    return 1;\n"
    output += " }\n\n"
    index = 3 
    output += "  RequestType requestType = (RequestType) atoi(argv[1]);\n"
    output += "  query_id = std::string(argv[2]);\n"
    for argument in arguments:
        output += "  int {} = atoi(argv[{}]);\n".format(argument, index)
        index += 1

    output += "  std::cout << \"argc: \" << argc << std::endl;\n"
    output += "  if (argc >= {}) {{\n".format(index+1)
    #output += "  if (argc == {}) {{\n".format(index+2)
    output += "    cache.set_cache_servers(argv[{}]);\n".format(index)
    #output += "    maxCacheToUse = atol(argv[{}]);\n".format(index+1)
    output += "  }\n"
    index += 1

    output += "  if (argc >= {}) {{\n".format(index+1)
    #output += "  if (argc == {}) {{\n".format(index+2)
    output += "    cache.set_cache_inputs(argv[{}]);\n".format(index)
    #output += "    maxCacheToUse = atol(argv[{}]);\n".format(index+1)
    output += "  }\n"
    return output

def gen_base_table_scan_aggregate_header(root, operator):
    template_text = ""
    task_header_template = ""
    with open("base_table_scan_aggregate_header.template", "r") as template:
        template_text = template.read()
    with open("read_base_table_task_header.template", "r") as template:
        task_header_template = template.read()
    params=", ".join(["", "int outPartitions", "std::unordered_map<struct {group_by_type}, struct {aggregate_type}> *agg_map".format(group_by_type="{}_group_by_record".format(get_request_name(operator).lower()), aggregate_type="{}_agg_record".format(get_request_name(operator).lower()))])

    out_text = task_header_template.format(class_name=root["class_name"], task_name=operator["stage_name"]+"Task", params=params)
    out_text += template_text.format(class_name=root["class_name"], stage_name=operator["stage_name"], out_partitions_param="int outPartitions, " if "partition_column" in operator else "")

    return out_text 

def gen_reduce_agg(operator):
    output = ""
    output += " "*2+"for (auto it : local_agg_map){\n"
    output += " "*4+"auto &in_agg_rec = it.second;\n"
    output += " "*4+"muxs[0].lock();\n"
    if "agg_rec_initialization" in operator:
        output += " "*4+"auto agg_it = out_agg_map->find(it.first);\n"
        output += " "*4+"if (agg_it == out_agg_map->end()) {\n"
        output += " "*6+"agg_it = out_agg_map->insert(agg_it, {{it.first, {}}});\n".format(operator["agg_rec_initialization"])
        output += " "*4+"}\n"
        output += " "*4+"struct {}_agg_record &agg_rec = agg_it->second;\n".format(get_request_name(operator).lower())

    else:
        output += " "*4+"auto &agg_rec = (*out_agg_map)[it.first];\n"
    output += ("\n"+" "*4).join([""]+operator["reduce_aggregate"].split("\n"))
    output += " "*4+"muxs[0].unlock();\n"
    output += " "*2+"}\n"
    return output


def gen_base_table_scan_aggregate_cpp(root, operator, operator_idx):
    template_text = ""
    task_template = ""
    launcher_template = ""
    with open("base_table_scan_aggregate.template", "r") as template:
        template_text = template.read()
    with open("read_base_table_task.template", "r") as template:
        task_template = template.read()
    with open("read_base_table_launch.template", "r") as template:
        launch_template = template.read()
    task_name = operator["stage_name"]+"Task"

    task_dict = {}
    task_dict["class_name"] = root["class_name"] 
    task_dict["stage_name"] = operator["stage_name"]
    task_dict["task_name"] = task_name
    task_dict["params"] = ", ".join(["", "int outPartitions", "std::unordered_map<struct {group_by_type}, struct {aggregate_type}> *out_agg_map".format(group_by_type="{}_group_by_record".format(get_request_name(operator).lower()), aggregate_type="{}_agg_record".format(get_request_name(operator).lower()))])
    task_dict["table_s3_bucket"]=root["table_s3_bucket"]
    task_dict["selected_columns"] = gen_selected_columns(operator)
    task_dict["vector_batch_definitions"]=gen_vector_batch(operator["in_columns"])
    task_dict["row_data"]=gen_row_data(operator["in_columns"])
    task_dict["row_processing"]=create_agg_record_base_table(operator)
    task_dict["filter_condition"]=operator["filter_condition"]
    task_dict["additional_initialization"]="std::unordered_map<struct {group_by_type}, struct {aggregate_type}> local_agg_map;\n".format(group_by_type="{}_group_by_record".format(get_request_name(operator).lower()), aggregate_type="{}_agg_record".format(get_request_name(operator).lower()))
    task_dict["post_row_processing"]=gen_reduce_agg(operator)
    tasks = task_template.format(**task_dict)

    ln_dict = {}
    ln_dict["class_name"] = root["class_name"] 
    ln_dict["stage_name"] = operator["stage_name"]
    ln_dict["task_name"] = task_name
    ln_dict["in_file_format"] = operator["in_file"]
    ln_dict["table_s3_bucket"]=root["table_s3_bucket"]
    ln_dict["tpool_name"] = "tpool"
    ln_dict["params"] = ", ".join(["", "outPartitions", "agg_map.get()"])
    ln_dict["split"] = operator["splits"] if "splits" in operator else "1"
    launch = launch_template.format(**ln_dict)

    tt_dict = {}
    tt_dict["class_name"] = root["class_name"] 
    tt_dict["stage_name"] = operator["stage_name"]
    tt_dict["in_file_format"]=operator["in_file"]
    tt_dict["intermediate_s3_bucket"]=root["intermediate_s3_bucket"]
    tt_dict["operator_idx"]=operator_idx
    tt_dict["tasks"]=tasks
    tt_dict["launch_tasks"]=launch
    tt_dict["group_by_type"]="{}_group_by_record".format(get_request_name(operator).lower())
    tt_dict["aggregate_type"]="{}_agg_record".format(get_request_name(operator).lower())
    tt_dict["output_type"]="{}_record".format(get_request_name(operator).lower())
    tt_dict["request_name"]=get_request_name(operator)
    tt_dict["convert_partitioned_data"]=gen_convert_output(root).format(operator_idx=operator_idx)
    tt_dict["limit"] = "LONG_MAX" if "limit" not in operator else str(operator["limit"])+"L"
    tt_dict["having_condition"] = "true" if "having_condition" not in operator else operator["having_condition"]

    if "partition_column" in operator:
        partition_column_hash = "out_rec."+operator["partition_column"]
        columns = operator["group_by_columns"]+operator["aggregate_columns"]
        index = [x[0] for x in columns].index(operator["partition_column"])
        if columns[index][1] == "STRING":
            partition_column_hash = "std::hash<std::string>()(std::string(out_rec.{}, {}))".format(columns[index][0], columns[index][2])


        tt_dict["set_partition"] = "partition_num = ({} % 91397) % outPartitions;".format(partition_column_hash)
        tt_dict["num_partitions"] = "outPartitions"
        tt_dict["out_partitions_param"] = "int outPartitions, "
    else:
        tt_dict["set_partition"] = ""
        tt_dict["num_partitions"] = 1;
        tt_dict["out_partitions_param"] = ""

    out_text = template_text.format(**tt_dict)
 
    return out_text

def gen_base_table_scan_partition_header(root, operator):
    template_text = ""
    task_header_template = ""
    with open("base_table_scan_partition_header.template", "r") as template:
        template_text = template.read()
    with open("read_base_table_task_header.template", "r") as template:
        task_header_template = template.read()

    params=", ".join(["", "int outPartitions", "std::vector<SinglePartitionData> *partitionData"])

    out_text = task_header_template.format(class_name=root["class_name"], task_name=operator["stage_name"]+"Task", params=params)
    out_text += template_text.format(class_name=root["class_name"], stage_name=operator["stage_name"])

    return out_text 


def gen_base_table_scan_partition_cpp(root, operator, operator_idx):
    template_text = ""
    task_template = ""
    if "alias" in operator:
        for column in operator["in_columns"]:
            column[0] = operator["alias"] + "_" + column[0]
    launcher_template = ""
    with open("base_table_scan_partition.template", "r") as template:
        template_text = template.read()
    with open("read_base_table_task.template", "r") as template:
        task_template = template.read()
    with open("read_base_table_launch.template", "r") as template:
        launch_template = template.read()
    task_name = operator["stage_name"]+"Task"

    task_dict = {}
    task_dict["class_name"] = root["class_name"] 
    task_dict["stage_name"] = operator["stage_name"]
    task_dict["task_name"] = task_name
    task_dict["params"] = ", ".join(["", "int outPartitions", "std::vector<SinglePartitionData> *partitionData"])
    task_dict["table_s3_bucket"]=root["table_s3_bucket"]
    task_dict["selected_columns"] = gen_selected_columns(operator)
    task_dict["vector_batch_definitions"]=gen_vector_batch(operator["in_columns"])
    task_dict["row_data"]=gen_row_data(operator["in_columns"])
    task_dict["row_processing"]=gen_scan_row_processing(operator)
    task_dict["filter_condition"]=operator["filter_condition"]
    task_dict["additional_initialization"]=""
    task_dict["post_row_processing"]=""
    tasks = task_template.format(**task_dict)

    ln_dict = {}
    ln_dict["class_name"] = root["class_name"] 
    ln_dict["stage_name"] = operator["stage_name"]
    ln_dict["task_name"] = task_name
    ln_dict["in_file_format"] = operator["in_file"]
    ln_dict["table_s3_bucket"]=root["table_s3_bucket"]
    ln_dict["tpool_name"] = "tpool"
    ln_dict["params"] = ", ".join(["", "outPartitions", "&partitionData"])
    ln_dict["split"] = operator["splits"] if "splits" in operator else "1"
    launch = launch_template.format(**ln_dict)

    tt_dict = {}
    tt_dict["class_name"] = root["class_name"] 
    tt_dict["stage_name"] = operator["stage_name"]
    tt_dict["in_file_format"]=operator["in_file"]
    tt_dict["intermediate_s3_bucket"]=root["intermediate_s3_bucket"]
    tt_dict["operator_idx"]=operator_idx
    tt_dict["output_dictionaries"]=gen_output_dictionaries(operator)
    tt_dict["tasks"]=tasks
    tt_dict["launch_tasks"]=launch
    tt_dict["convert_partitioned_data"]=gen_convert_output(root).format(operator_idx=operator_idx)

    out_text = template_text.format(**tt_dict)
 
    return out_text;

def gen_convert_output(root):
    format_file = ""
    if "compress" in root and root["compress"].lower() == "on":
        format_file = "compressed_outputs.template"
    else:
        format_file = "compressed_outputs.template"
        format_file = "raw_outputs.template"
    with open(format_file, "r") as template:
        return template.read();

def gen_partitioned_hash_join_aggregate_header(operator, operator_mapping):
    left_operator = operator_mapping[operator["left_input"]]
    template_text = ""
    task_header_template = ""
    with open("partitioned_hash_join_aggregate_header.template", "r") as template:
        template_text = template.read()
    with open("read_partitioned_file_task_header.template", "r") as template:
        task_header_template = template.read()
    build_task_name = operator["stage_name"]+"BuildTask"
    probe_task_name = operator["stage_name"]+"ProbeTask"
    broadcast_params = []
    out_text = ""
    if "broadcast_inputs" in operator:
        index = 1
        for bcast_input in operator["broadcast_inputs"]:
            record_type = get_record_type(operator_mapping[bcast_input[0]])
            curr_param = "std::unordered_map<int64_t, std::vector<struct {record_type}>> *bcast_hash_map{index}".format(record_type=get_record_type(operator_mapping[bcast_input[0]]), index=index )
            out_text += task_header_template.format(task_name=operator["stage_name"]+"BCastBuild{}".format(index), in_parameters=", ".join(["", curr_param]))
            broadcast_params.append(curr_param)
            index+=1
        
    out_text += task_header_template.format(task_name=build_task_name, in_parameters=", ".join(["", "std::unordered_map<int64_t, std::vector<struct {left_record_type}>> *hash_map".format(left_record_type=get_record_type(left_operator))]))
    out_text += task_header_template.format(task_name=probe_task_name, in_parameters= ", ".join(["", "std::unordered_map<int64_t, std::vector<struct {left_record_type}>> *hash_map".format(left_record_type=get_record_type(left_operator)), "std::unordered_map<struct {group_by_type}, struct {aggregate_type}> *agg_map".format(group_by_type="{}_group_by_record".format(get_request_name(operator).lower()), aggregate_type="{}_agg_record".format(get_request_name(operator).lower()))]+broadcast_params))
    out_text += template_text.format(stage_name=operator["stage_name"], left_record_type=get_record_type(left_operator), group_by_type="{}_group_by_record".format(get_request_name(operator).lower()), aggregate_type="{}_agg_record".format(get_request_name(operator).lower()), out_partitions_param="int outPartitions," if "partition_column" in operator else "")

    return out_text


def gen_final_aggregate_header(class_name, operator, in_operator):
    template_text = ""
    with open("final_aggregate_header.template", "r") as template:
        template_text = template.read()
    return template_text.format(stage_name=operator["stage_name"], group_by_type="{}_group_by_record".format(get_request_name(in_operator).lower()), aggregate_type="{}_agg_record".format(get_request_name(in_operator).lower()))

def get_max_encoding_size(operator, operator_mapping):
    if operator["type"] in ["Combiner", "FinalAggregate"]:
        operator = operator_mapping[operator["in_operator"]]
    max_size = ""
    for column in operator["out_columns"]:
        if len(column) >=4 and column[3] == "DICT":
            max_size += " 1+{}_DICT_RECORD_SIZE*256 +".format(column[0].upper())
    max_size+= "0"
    return max_size

def get_local_dicts(operator, operator_mapping):
    if operator["type"] == "Combiner":
        operator = operator_mapping[operator["in_operator"]]
    output = ""
    for column in operator["out_columns"]:
        if len(column) >=4 and column[3] == "DICT":
            output += "  std::unordered_map<uint8_t, uint8_t> {}_l_to_g;\n".format(column[0].lower())
    return output

def get_record_type(operator):
    base_name = ""
    if "type" in operator and operator["type"] in ["Combiner", "FinalAggregate"]:
        base_name = get_request_name_from_name(operator["in_operator"])
    else:
        base_name = get_request_name(operator)
    return base_name.lower()+"_record"

def get_populate_dicts(operator, operator_mapping):
    if operator["type"] == "Combiner":
        operator = operator_mapping[operator["in_operator"]]
    output = ""
    for column in operator["out_columns"]:
        if len(column) >=4 and column[3] == "DICT":
            output += " "*4+"std::memcpy((char *)&numDictEntries, buf+currIdx, sizeof(numDictEntries));\n"
            output += " "*4+"currIdx += sizeof(numDictEntries);\n"
            output += " "*4+"for (int dictEntryIdx = 0; dictEntryIdx < numDictEntries; ++dictEntryIdx){\n"
            output += " "*6+"struct {}_dict_record op{{}};\n".format(column[0])
            output += " "*6+"std::memcpy(reinterpret_cast<char*>(&op), buf+currIdx, sizeof(op));\n"
            output += " "*6+"currIdx += sizeof(op);\n"
            output += " "*6+"muxs[0].lock();\n"
            output += " "*6+"std::string dict_str(op.{}, sizeof(op.{}));\n".format(column[0], column[0])
            output += " "*6+"if ({}_dict_encoding_write.find(dict_str) == {}_dict_encoding_write.end()){{\n".format(column[0], column[0])
            output += " "*8+"auto enc = {}_curr_encoding++;\n".format(column[0], column[0])
            output += " "*8+"{}_dict_encoding_write[dict_str] = enc;\n".format(column[0], column[0])
            output += " "*8+"{}_dict_encoding_read[enc] = dict_str;\n".format(column[0], column[0])
            output += " "*6+"}\n"
            output += " "*6+"{}_l_to_g[op.enc] = {}_dict_encoding_write[dict_str];\n".format(column[0], column[0])

            output += " "*6+"muxs[0].unlock();\n"
            output += " "*4+"}\n\n"

    # set total_encoding_size
    return output

def get_record_size(operator):
    base_name = ""
    if operator["type"] in ["Combiner", "FinalAggregate"]:
        base_name = get_request_name_from_name(operator["in_operator"])
    else:
        base_name = get_request_name(operator)
    return base_name+"_RECORD_SIZE"

def get_remap_dicts(operator, operator_mapping, rec_name):
    if operator["type"] == "Combiner":
        operator = operator_mapping[operator["in_operator"]]
    output = ""
    for column in operator["out_columns"]:
        if len(column) >=4 and column[3] == "DICT":
            output += " "*6+"{}.{}_enc = {}_l_to_g[{}.{}_enc];\n".format(rec_name, column[0], column[0], rec_name, column[0])

    return output

def gen_output_record(operator, right_rec_name="right_rec.", left_rec_name="left_rec."):
    output = "//gen_output_record\n"
    for column in operator["out_columns"]:
        source = right_rec_name if column[-1] == "right" else left_rec_name
        if column[1] == "STRING":
            output+="//string {} {}\n".format(len(column), column[3]=="DICT")
            if len(column) >= 4 and column[3] == "DICT":
                if operator["type"] in ["PartitionedHashJoin", "TwoBaseTableJoinPartition", "BroadcastJoinBaseTable"] and column[-1] == "left":
                    output += " "*8 + "out_rec.{}_enc = {}_enc;\n".format(column[0], source+column[0])
                else:
                    output += " "*8 + "if ({}_dict_encoding_write.find({}) == {}_dict_encoding_write.end()){{\n".format(column[0], column[0], source+column[0])
                    output += " "*10 + "{}_dict_encoding_write[{}] = {}_curr_encoding++;\n".format(column[0], column[0], source+column[0])
                    output += " "*8 + "}\n"
                    output += " "*8 + "out_rec.{}_enc = {}_dict_encoding_write[{}];\n".format(column[0], column[0], source+column[0])
            else:
                from_var = source+column[0]
                if operator["type"] in ["TwoBaseTableJoinPartition", "BroadcastJoinBaseTable"] and column[-1] == "right":
                    from_var += ".data()"

                output += " "*8+ "std::memcpy(out_rec.{}, {}, {});\n".format(column[0], from_var, int(column[2]))
        else:
            output += " "*8+"out_rec.{} = {};\n".format(column[0], source+column[0])
    return output

def gen_broadcast_join_base_table_header(operator, operator_mapping):
    left_operator = operator_mapping[operator["left_input"]]
    template_text = ""
    partitioned_file_header_template = ""
    base_table_header_template = ""
    with open("broadcast_join_base_table_header.template", "r") as template:
        template_text = template.read()
    with open("read_partitioned_file_task_header.template", "r") as template:
        partitioned_file_header_template = template.read()
    with open("read_base_table_task_header.template", "r") as template:
        base_table_header_template = template.read()
    build_task_name = operator["stage_name"]+"BuildTask"
    probe_task_name = operator["stage_name"]+"ProbeTask"
    out_text = partitioned_file_header_template.format(task_name=build_task_name, in_parameters=", ".join(["", "std::unordered_map<int64_t, std::vector<struct {left_record_type}>> *hash_map".format(left_record_type=get_record_type(left_operator))]))
    out_text += base_table_header_template.format(task_name=probe_task_name, params=", ".join(["", "int inPartitions", "int outPartitions", "std::unordered_map<int64_t, std::vector<struct {left_record_type}>> *hash_map".format(left_record_type=get_record_type(left_operator)), "std::vector<SinglePartitionData> *partitionData"]))
    out_text += template_text.format(stage_name=operator["stage_name"], left_record_type=get_record_type(left_operator))
    return out_text


def gen_broadcast_join_base_table_cpp(root, operator, operator_idx, operator_mapping, operator_idx_mapping):
    left_operator = operator_mapping[operator["left_input"]]
    left_operator_idx = operator_idx_mapping[operator["left_input"]]
    right_operator = operator["right_input"]

    template_text = ""
    partitioned_task_template = ""
    partitioned_launch_template = ""
    base_table_task_template = ""
    base_table_launch_template = ""
    with open("broadcast_join_base_table.template", "r") as template:
        template_text = template.read()
    with open("read_partitioned_file_task.template", "r") as template:
        partitioned_task_template = template.read()
    with open("read_base_table_task.template", "r") as template:
        base_table_task_template = template.read()
    with open("read_partitioned_file_launch.template", "r") as template:
        partitioned_file_launch_template = template.read()
    with open("read_base_table_launch.template", "r") as template:
        base_table_launch_template = template.read()
    build_task_name = operator["stage_name"]+"BuildTask"
    probe_task_name = operator["stage_name"]+"ProbeTask"

    task_dict = {}
    task_dict["class_name"] = root["class_name"] 
    task_dict["stage_name"] = operator["stage_name"]
    task_dict["task_name"] = build_task_name
    task_dict["in_parameters"] = ", ".join(["", "std::unordered_map<int64_t, std::vector<struct {left_record_type}>> *hash_map".format(left_record_type=get_record_type(left_operator))])
    task_dict["intermediate_s3_bucket"]=root["intermediate_s3_bucket"]
    task_dict["additional_initialization"] = ""
    task_dict["row_processing"]=join_build_phase_process_record(operator, operator_mapping)
    task_dict["in_rec_name"] = "left_rec"
    task_dict["in_max_encoding_size"]=get_max_encoding_size(left_operator, operator_mapping)
    task_dict["in_record_type"]=get_record_type(left_operator)
    task_dict["in_record_size"]=get_record_size(left_operator)
    task_dict["in_local_dicts"]=get_local_dicts(left_operator, operator_mapping)
    task_dict["in_populate_dicts"]=get_populate_dicts(left_operator, operator_mapping)
    task_dict["in_remap_dicts"]=get_remap_dicts(left_operator, operator_mapping, "left_rec")
    task_dict["in_operator_idx"]=left_operator_idx
    task_dict["uncompress"]=gen_uncompress(root)
    task_dict["post_row_processing"] = ""
    tasks = partitioned_task_template.format(**task_dict)

    task_dict = {}
    task_dict["class_name"] = root["class_name"] 
    task_dict["stage_name"] = operator["stage_name"]
    task_dict["task_name"] = probe_task_name
    task_dict["params"] = ", ".join(["", "int inPartitions", "int outPartitions", "std::unordered_map<int64_t, std::vector<struct {left_record_type}>> *hash_map".format(left_record_type=get_record_type(left_operator)), "std::vector<SinglePartitionData> *partitionData"])
    task_dict["table_s3_bucket"]=root["table_s3_bucket"]
    task_dict["selected_columns"] = gen_selected_columns(right_operator)
    task_dict["vector_batch_definitions"]=gen_vector_batch(right_operator["in_columns"])
    task_dict["row_data"]=gen_row_data(right_operator["in_columns"])
    task_dict["row_processing"]=partition_join_probe_phase_process_record(operator, operator_mapping, "left_rec.", "")
    task_dict["filter_condition"]=right_operator["filter_condition"]
    task_dict["additional_initialization"] = "struct {right_record_type} out_rec;\n".format(right_record_type=get_record_type(operator))
    task_dict["post_row_processing"] = ""
    tasks += base_table_task_template.format(**task_dict)

    right_start_idx = "0"
    right_jump_idx = 1
    left_start_idx = "0"
    left_jump_idx = 1
    task_divisor = 1
    should_drop_left="true"
    left_cache_read_idx="taskNum"
    should_drop_right="true"
    right_cache_read_idx="taskNum"
    if "split_join" in operator:
        split_join = operator["split_join"]
        if split_join[0] == "right":
            right_jump_idx = split_join[1]
            right_start_idx = "taskNum%{}".format(split_join[1])
        else:
            left_jump_idx = split_join[1]
            left_start_idx = "taskNum%{}".format(split_join[1])
        task_divisor = split_join[1]
    if "type" in right_operator and right_operator["type"] == "Combiner":
        right_start_idx = "taskNum/(inPartitions/{})".format(right_operator["partition_factor"])
        right_jump_idx = right_operator["partition_factor"]
    elif "type" in right_operator and right_operator["type"] == "FinalAggregate":
        right_start_idx = "taskNum"
        right_jump_idx = "numLeftFiles"
        should_drop_right = "false"
        right_cache_read_idx = "0"

    if "type" in left_operator and left_operator["type"] == "Combiner":
        left_start_idx = "taskNum/(inPartitions/{})".format(left_operator["partition_factor"])
        left_jump_idx = left_operator["partition_factor"]
    elif "type" in left_operator and left_operator["type"] == "FinalAggregate":
        left_start_idx = "taskNum"
        left_jump_idx = "numLeftFiles"
        should_drop_left = "false"
        left_cache_read_idx = "0"

    ln_dict = {}
    ln_dict["class_name"] = root["class_name"] 
    ln_dict["stage_name"] = operator["stage_name"]
    ln_dict["task_name"] = build_task_name
    ln_dict["tpool_name"] = "tpool1"
    ln_dict["numInFiles"] = "1"
    ln_dict["in_partition_id"] = 0
    ln_dict["in_partitions"] = "inPartitions"
    ln_dict["start_idx"] = left_start_idx
    ln_dict["jump_idx"] = left_jump_idx
    ln_dict["task_divisor"] = task_divisor
    ln_dict["params"] = ", ".join(["", "hash_map.get()"])
    ln_dict["in_operator_idx"] = left_operator_idx
    ln_dict["should_drop"]=should_drop_left if "drop_left" not in operator else operator["drop_left"] 
    ln_dict["cache_read_idx"] = 0
    build_task_launch = partitioned_file_launch_template.format(**ln_dict)

    ln_dict = {}
    ln_dict["table_s3_bucket"] = root["table_s3_bucket"]
    ln_dict["in_file_format"] = right_operator["in_file"]
    ln_dict["class_name"] = root["class_name"] 
    ln_dict["stage_name"] = operator["stage_name"]
    ln_dict["task_name"] = probe_task_name
    ln_dict["tpool_name"] = "tpool2"
    ln_dict["in_partitions"] = "inPartitions"
    ln_dict["start_idx"] = right_start_idx
    ln_dict["jump_idx"] = right_jump_idx
    ln_dict["task_divisor"] = task_divisor
    ln_dict["params"] = ", ".join(["", "1", "outPartitions", "hash_map.get()", "&partitionData"])
    ln_dict["split"] = "1"
    probe_task_launch = base_table_launch_template.format(**ln_dict)

    tt_dict = {}
    tt_dict["class_name"] = root["class_name"] 
    tt_dict["stage_name"] = operator["stage_name"]
    tt_dict["operator_idx"]=operator_idx
    tt_dict["output_dictionaries"]=gen_output_dictionaries(operator)
    tt_dict["tasks"]=tasks
    tt_dict["build_task_launch"]=build_task_launch
    tt_dict["probe_task_launch"]=probe_task_launch
    tt_dict["left_record_type"]=get_record_type(left_operator)
    tt_dict["convert_partitioned_data"]=gen_convert_output(root).format(operator_idx=operator_idx)

    out_text = template_text.format(**tt_dict)
 
    return out_text

def gen_two_base_table_join_partition_header(operator, operator_mapping):
    left_operator = operator["left_input"]
    template_text = ""
    base_table_header_template = ""
    with open("two_base_table_join_header.template", "r") as template:
        template_text = template.read()
    with open("read_base_table_task_header.template", "r") as template:
        base_table_header_template = template.read()
    build_task_name = operator["stage_name"]+"BuildTask"
    probe_task_name = operator["stage_name"]+"ProbeTask"
    out_text = base_table_header_template.format(task_name=build_task_name, params=", ".join(["", "std::unordered_map<int64_t, std::vector<struct {left_record_type}>> *hash_map".format(left_record_type=get_record_type(left_operator))]))
    out_text += base_table_header_template.format(task_name=probe_task_name, params=", ".join(["", "int outPartitions", "std::unordered_map<int64_t, std::vector<struct {left_record_type}>> *hash_map".format(left_record_type=get_record_type(left_operator)), "std::vector<SinglePartitionData> *partitionData"]))
    out_text += template_text.format(stage_name=operator["stage_name"], left_record_type=get_record_type(left_operator))
    return out_text


def gen_two_base_table_join_partition_cpp(root, operator, operator_idx, operator_mapping, operator_idx_mapping):
    left_operator = operator["left_input"]
    right_operator = operator["right_input"]
    
    if "alias" in left_operator:
        for column in left_operator["in_columns"]:
            column[0] = left_operator["alias"] + "_" + column[0]
    if "alias" in right_operator:
        for column in right_operator["in_columns"]:
            column[0] = right_operator["alias"] + "_" + column[0]

    template_text = ""
    base_table_task_template = ""
    base_table_launch_template = ""
    with open("two_base_table_join.template", "r") as template:
        template_text = template.read()
    with open("read_base_table_task.template", "r") as template:
        base_table_task_template = template.read()
    with open("read_base_table_launch.template", "r") as template:
        base_table_launch_template = template.read()
    build_task_name = operator["stage_name"]+"BuildTask"
    probe_task_name = operator["stage_name"]+"ProbeTask"

    task_dict = {}
    task_dict = {}
    task_dict["class_name"] = root["class_name"] 
    task_dict["stage_name"] = operator["stage_name"]
    task_dict["task_name"] = build_task_name
    task_dict["params"] = ", ".join(["", "std::unordered_map<int64_t, std::vector<struct {left_record_type}>> *hash_map".format(left_record_type=get_record_type(left_operator))])
    task_dict["table_s3_bucket"]=root["table_s3_bucket"]
    task_dict["selected_columns"] = gen_selected_columns(left_operator)
    task_dict["vector_batch_definitions"]=gen_vector_batch(left_operator["in_columns"])
    task_dict["row_data"]=gen_row_data(left_operator["in_columns"])
    task_dict["row_processing"]=generate_scan_record(left_operator) + join_build_phase_process_record(operator, operator_mapping, "out_rec")
    task_dict["filter_condition"]=left_operator["filter_condition"]
    task_dict["additional_initialization"] = "int outPartitions = 1;\nstruct {right_record_type} out_rec;\n".format(right_record_type=get_record_type(operator))
    task_dict["post_row_processing"] = ""
    tasks = base_table_task_template.format(**task_dict)

    task_dict = {}
    task_dict["class_name"] = root["class_name"] 
    task_dict["stage_name"] = operator["stage_name"]
    task_dict["task_name"] = probe_task_name
    task_dict["params"] = ", ".join(["", "int outPartitions", "std::unordered_map<int64_t, std::vector<struct {left_record_type}>> *hash_map".format(left_record_type=get_record_type(left_operator)), "std::vector<SinglePartitionData> *partitionData"])
    task_dict["table_s3_bucket"]=root["table_s3_bucket"]
    task_dict["selected_columns"] = gen_selected_columns(right_operator)
    task_dict["vector_batch_definitions"]=gen_vector_batch(right_operator["in_columns"])
    task_dict["row_data"]=gen_row_data(right_operator["in_columns"])
    task_dict["row_processing"]=partition_join_probe_phase_process_record(operator, operator_mapping, "left_rec.", "")
    task_dict["filter_condition"]=right_operator["filter_condition"]
    task_dict["additional_initialization"] = "struct {right_record_type} out_rec;\n".format(right_record_type=get_record_type(operator))
    task_dict["post_row_processing"] = ""
    tasks += base_table_task_template.format(**task_dict)

    ln_dict = {}
    ln_dict["table_s3_bucket"] = root["table_s3_bucket"]
    ln_dict["in_file_format"] = left_operator["in_file"]
    ln_dict["class_name"] = root["class_name"] 
    ln_dict["stage_name"] = operator["stage_name"]
    ln_dict["task_name"] = build_task_name
    ln_dict["tpool_name"] = "tpool1"
    ln_dict["params"] = ", ".join(["", "hash_map.get()"])
    ln_dict["split"] = "1"
    build_task_launch = base_table_launch_template.format(**ln_dict)

    ln_dict = {}
    ln_dict["table_s3_bucket"] = root["table_s3_bucket"]
    ln_dict["in_file_format"] = right_operator["in_file"]
    ln_dict["class_name"] = root["class_name"] 
    ln_dict["stage_name"] = operator["stage_name"]
    ln_dict["task_name"] = probe_task_name
    ln_dict["tpool_name"] = "tpool2"
    ln_dict["params"] = ", ".join(["", "outPartitions", "hash_map.get()", "&partitionData"])
    ln_dict["split"] = "1"
    probe_task_launch = base_table_launch_template.format(**ln_dict)

    tt_dict = {}
    tt_dict["class_name"] = root["class_name"] 
    tt_dict["stage_name"] = operator["stage_name"]
    tt_dict["operator_idx"]=operator_idx
    tt_dict["output_dictionaries"]=gen_output_dictionaries(operator)
    tt_dict["tasks"]=tasks
    tt_dict["build_task_launch"]=build_task_launch
    tt_dict["probe_task_launch"]=probe_task_launch
    tt_dict["left_record_type"]=get_record_type(left_operator)
    tt_dict["convert_partitioned_data"]=gen_convert_output(root).format(operator_idx=operator_idx)

    out_text = template_text.format(**tt_dict)
 
    return out_text

def gen_partitioned_hash_join_header(operator, operator_mapping):
    left_operator = operator_mapping[operator["left_input"]]
    template_text = ""
    task_header_template = ""
    with open("partitioned_hash_join_header.template", "r") as template:
        template_text = template.read()
    with open("read_partitioned_file_task_header.template", "r") as template:
        task_header_template = template.read()
    build_task_name = operator["stage_name"]+"BuildTask"
    probe_task_name = operator["stage_name"]+"ProbeTask"
    out_text = task_header_template.format(task_name=build_task_name, in_parameters=", ".join(["", "std::unordered_map<int64_t, std::vector<struct {left_record_type}>> *hash_map".format(left_record_type=get_record_type(left_operator))]))
    out_text += task_header_template.format(task_name=probe_task_name, in_parameters=", ".join(["", "int outPartitions", "std::unordered_map<int64_t, std::vector<struct {left_record_type}>> *hash_map".format(left_record_type=get_record_type(left_operator)), "std::vector<SinglePartitionData> *partitionData"]))
    out_text += template_text.format(stage_name=operator["stage_name"], left_record_type=get_record_type(left_operator), out_partitions_param="int outPartitions," if "partition_column" in operator else "")
    return out_text


def gen_partitioned_hash_join_cpp(root, operator, operator_idx, operator_mapping, operator_idx_mapping):
    left_operator = operator_mapping[operator["left_input"]]
    left_operator_idx = operator_idx_mapping[operator["left_input"]]
    right_operator = operator_mapping[operator["right_input"]]
    right_operator_idx = operator_idx_mapping[operator["right_input"]]

    template_text = ""
    task_template = ""
    launcher_template = ""
    with open("partitioned_hash_join.template", "r") as template:
        template_text = template.read()
    with open("read_partitioned_file_task.template", "r") as template:
        task_template = template.read()
    with open("read_partitioned_file_launch.template", "r") as template:
        launch_template = template.read()
    build_task_name = operator["stage_name"]+"BuildTask"
    probe_task_name = operator["stage_name"]+"ProbeTask"

    task_dict = {}
    task_dict["class_name"] = root["class_name"] 
    task_dict["stage_name"] = operator["stage_name"]
    task_dict["task_name"] = build_task_name
    task_dict["in_parameters"] = ", ".join(["", "std::unordered_map<int64_t, std::vector<struct {left_record_type}>> *hash_map".format(left_record_type=get_record_type(left_operator))])
    task_dict["intermediate_s3_bucket"]=root["intermediate_s3_bucket"]
    task_dict["additional_initialization"] = ""
    task_dict["row_processing"]=join_build_phase_process_record(operator, operator_mapping)
    task_dict["in_rec_name"] = "left_rec"
    task_dict["in_max_encoding_size"]=get_max_encoding_size(left_operator, operator_mapping)
    task_dict["in_record_type"]=get_record_type(left_operator)
    task_dict["in_record_size"]=get_record_size(left_operator)
    task_dict["in_local_dicts"]=get_local_dicts(left_operator, operator_mapping)
    task_dict["in_populate_dicts"]=get_populate_dicts(left_operator, operator_mapping)
    task_dict["in_remap_dicts"]=get_remap_dicts(left_operator, operator_mapping, "left_rec")
    task_dict["in_operator_idx"]=left_operator_idx
    task_dict["uncompress"]=gen_uncompress(root)
    task_dict["post_row_processing"]=""
    tasks = task_template.format(**task_dict)

    task_dict = {}
    task_dict["class_name"] = root["class_name"] 
    task_dict["stage_name"] = operator["stage_name"]
    task_dict["task_name"] = probe_task_name
    task_dict["in_parameters"] = ", ".join(["", "int outPartitions", "std::unordered_map<int64_t, std::vector<struct {left_record_type}>> *hash_map".format(left_record_type=get_record_type(left_operator)), "std::vector<SinglePartitionData> *partitionData"])
    task_dict["intermediate_s3_bucket"]=root["intermediate_s3_bucket"]
    task_dict["additional_initialization"] = "struct {out_record_type} out_rec;\n".format(out_record_type=get_record_type(operator))
    task_dict["row_processing"]=partition_join_probe_phase_process_record(operator, operator_mapping)
    task_dict["in_rec_name"] = "right_rec"
    task_dict["in_max_encoding_size"]=get_max_encoding_size(right_operator, operator_mapping)
    task_dict["in_record_type"]=get_record_type(right_operator)
    task_dict["in_record_size"]=get_record_size(right_operator)
    task_dict["in_local_dicts"]=get_local_dicts(right_operator, operator_mapping)
    task_dict["in_populate_dicts"]=get_populate_dicts(right_operator, operator_mapping)
    task_dict["in_remap_dicts"]=get_remap_dicts(right_operator, operator_mapping, "right_rec")
    task_dict["in_operator_idx"]=right_operator_idx
    task_dict["uncompress"]=gen_uncompress(root)
    task_dict["post_row_processing"]=""

    tasks+= task_template.format(**task_dict)

    right_start_idx = "0"
    right_jump_idx = 1
    left_start_idx = "0"
    left_jump_idx = 1
    task_divisor = 1
    should_drop_left="true"
    left_cache_read_idx="taskNum"
    should_drop_right="true"
    right_cache_read_idx="taskNum"

    if "split_join" in operator:
        split_join = operator["split_join"]
        if split_join[0] == "right":
            right_jump_idx = split_join[1]
            right_start_idx = "taskNum%{}".format(split_join[1])
        else:
            left_jump_idx = split_join[1]
            left_start_idx = "taskNum%{}".format(split_join[1])
        task_divisor = split_join[1]
    if right_operator["type"] == "Combiner":
        right_start_idx = "taskNum/(inPartitions/{})".format(right_operator["partition_factor"])
        right_jump_idx = right_operator["partition_factor"]
    elif right_operator["type"] == "FinalAggregate":
        right_start_idx = "taskNum"
        right_jump_idx = "numRightFiles"
        should_drop_right="false"
        right_cache_read_idx="0"


    if left_operator["type"] == "Combiner":
        left_start_idx = "taskNum/(inPartitions/{})".format(left_operator["partition_factor"])
        left_jump_idx = left_operator["partition_factor"]
    elif left_operator["type"] == "FinalAggregate":
        left_start_idx = "taskNum"
        left_jump_idx = "numLeftFiles"
        should_drop_left="false"
        left_cache_read_idx="0"

    ln_dict = {}
    ln_dict["class_name"] = root["class_name"] 
    ln_dict["stage_name"] = operator["stage_name"]
    ln_dict["task_name"] = build_task_name
    ln_dict["tpool_name"] = "tpool1"
    ln_dict["numInFiles"] = "numLeftFiles"
    ln_dict["in_partition_id"] = "taskNum" if left_operator["type"] != "FinalAggregate" else "0"
    ln_dict["in_partitions"] = "inPartitions" if left_operator["type"] != "FinalAggregate" else "1"
    ln_dict["params"] = ", ".join(["", "hash_map.get()"])
    ln_dict["start_idx"] = left_start_idx
    ln_dict["jump_idx"] = left_jump_idx
    ln_dict["task_divisor"] = task_divisor
    ln_dict["in_operator_idx"]=left_operator_idx
    ln_dict["should_drop"]= should_drop_left if "drop_left" not in operator else operator["drop_left"] 
    ln_dict["cache_read_idx"]=left_cache_read_idx
    build_task_launch = launch_template.format(**ln_dict)

    ln_dict = {}
    ln_dict["class_name"] = root["class_name"] 
    ln_dict["stage_name"] = operator["stage_name"]
    ln_dict["task_name"] = probe_task_name
    ln_dict["tpool_name"] = "tpool2"
    ln_dict["numInFiles"] = "numRightFiles"
    ln_dict["in_partition_id"] = "taskNum" if right_operator["type"] != "FinalAggregate" else "0"
    ln_dict["in_partitions"] = "inPartitions" if right_operator["type"] != "FinalAggregate" else "1"
    ln_dict["params"] = ", ".join(["", "outPartitions", "hash_map.get()", "&partitionData"])
    ln_dict["start_idx"] = right_start_idx
    ln_dict["jump_idx"] = right_jump_idx
    ln_dict["task_divisor"] = task_divisor
    ln_dict["in_operator_idx"]=right_operator_idx
    ln_dict["should_drop"]= should_drop_right if "drop_right" not in operator else operator["drop_right"] 
    ln_dict["cache_read_idx"]=right_cache_read_idx
    probe_task_launch = launch_template.format(**ln_dict)

    tt_dict = {}
    tt_dict["class_name"] = root["class_name"] 
    tt_dict["stage_name"] = operator["stage_name"]
    tt_dict["operator_idx"]=operator_idx
    tt_dict["output_dictionaries"]=gen_output_dictionaries(operator)
    tt_dict["tasks"]=tasks
    tt_dict["build_task_launch"]=build_task_launch
    tt_dict["probe_task_launch"]=probe_task_launch
    tt_dict["left_record_type"]=get_record_type(left_operator)
    tt_dict["convert_partitioned_data"]=gen_convert_output(root).format(operator_idx=operator_idx)

    out_text = template_text.format(**tt_dict)
 
    return out_text

def create_agg_record(operator, operator_mapping):
    output = " "*12+"struct {}_group_by_record agg_key;\n".format(get_request_name(operator).lower())
    for column in operator["group_by_columns"]:
        currline = " "*12
        source_rec = ""
        if column[-1] == "right":
            source_rec = "right_rec"
        elif column[-1] == "left":
            source_rec = "left_rec"
        else:
            source_rec = column[-1]

        if (column[1] == "LONG" or column[1] == "DATE" or column[1] == "DOUBLE"):
            if column[-1] == "NONE":
                currline += "agg_key.{} = 0;\n".format(column[0]);
            else:
                currline += "agg_key.{} = {}.{};\n".format(column[0], source_rec, column[0]);

        elif(column[1] == "STRING"):
            if len(column) >= 4 and column[3] == "DICT":
                currline += "std::memcpy(agg_key.{col_name}, {col_name}_dict_encoding_read[{source_rec}.{col_name}_enc].data(), {col_len});\n".format(col_name=column[0], source_rec=source_rec, col_len=column[2]);
            else:
                currline += "std::memcpy(agg_key.{}, {}.{}, {});\n".format(column[0], source_rec, column[0], column[2]);
        output += currline
    right_operator = operator_mapping[operator["right_input"]]
    if right_operator["type"] == "Combiner":
        right_operator =  operator_mapping[right_operator["in_operator"]]
    partition_column = right_operator["partition_column"]
    if "exists" in operator and operator["exists"] == "right":
        output += " "*12+"muxs[0].lock();\n"
        output += " "*12+"if(!exists_set.insert(right_rec.{}).second){{\n".format(partition_column)
        output += " "*14+"muxs[0].unlock();\n"
        output += " "*14+"break;\n"
        output += " "*12+"}\n"
        output += " "*12+"muxs[0].unlock();\n"
    if "agg_rec_initialization" in operator:
        output += " "*12+"auto agg_it = local_agg_map.find(agg_key);\n"
        output += " "*12+"if (agg_it == local_agg_map.end()) {\n"
        output += " "*14+"agg_it = local_agg_map.insert(agg_it, {{agg_key, {}}});\n".format(operator["agg_rec_initialization"])
        output += " "*12+"}\n"
        output += " "*12+"struct {}_agg_record &agg_rec = agg_it->second;\n".format(get_request_name(operator).lower())

    else:
        output += " "*12+"struct {}_agg_record &agg_rec = local_agg_map[agg_key];\n".format(get_request_name(operator).lower())

    #output += " "*12+"(*agg_map)[agg_key].high_line_count++;\n".format(get_request_name(operator).lower())
    output += " "*12+operator["aggregate_code"]+"\n".replace("\n", "\n"+" "*12)

    return output

def create_agg_record_base_table(operator):
    output = " "*12+"struct {}_group_by_record agg_key{{}};\n".format(get_request_name(operator).lower())
    for column in operator["group_by_columns"]:
        currline = " "*12
        if (column[1] == "LONG" or column[1] == "DATE" or column[1] == "DOUBLE"):
            if column[-1] == "NONE":
                currline += "agg_key.{} = 0;\n".format(column[0]);
            else:
                currline += "agg_key.{} = {};\n".format(column[0],column[0]);

        elif(column[1] == "STRING"):
            currline += "std::memcpy(agg_key.{}, {}.data(), {});\n".format(column[0], column[0], column[2]);
        output += currline
    output += " "*12+"struct {}_agg_record &agg_rec = local_agg_map[agg_key];\n".format(get_request_name(operator).lower())
    #output += " "*12+"(*agg_map)[agg_key].high_line_count++;\n".format(get_request_name(operator).lower())
    output += " "*12+operator["aggregate_code"].replace("\n", "\n"+" "*12)

    return output

def create_final_agg_record(operator, in_operator):
    output = ""
    print(operator["stage_name"], in_operator["stage_name"])
    output += " "*6+in_operator["reduce_aggregate"].replace("\n", "\n"+" "*6)

    return output

def gen_broadcast_inputs(root, operator, operator_mapping, operator_idx_mapping):
    index = 1
    output = ""
    with open("read_partitioned_file_launch.template", "r") as template:
        launch_template = template.read()
    for bcast_input in operator["broadcast_inputs"]:
        bcast_input_idx = operator_idx_mapping[bcast_input[0]]
        record_type = get_record_type(operator_mapping[bcast_input[0]])
        output+="std::unique_ptr<std::unordered_map<int64_t, std::vector<struct {record_type}>>> bcast_hash_map{idx}(new std::unordered_map<int64_t, std::vector<struct {record_type}>>);".format(idx=index, record_type=record_type)
        ln_dict = {}
        ln_dict["class_name"] = root["class_name"] 
        ln_dict["stage_name"] = operator["stage_name"]
        ln_dict["task_name"] = operator["stage_name"]+"BCastBuild{}".format(index)
        ln_dict["tpool_name"] = "bcast_tpool{}".format(index)
        ln_dict["in_partition_id"] = "0"
        ln_dict["numInFiles"] = "1"
        ln_dict["in_partitions"] = "1"
        ln_dict["start_idx"] = 0
        ln_dict["jump_idx"] = 1
        ln_dict["task_divisor"] = 1
        ln_dict["params"] = ", ".join(["", "bcast_hash_map{}.get()".format(index)])
        ln_dict["in_operator_idx"] = bcast_input_idx
        ln_dict["should_drop"]="false"
        ln_dict["cache_read_idx"]="0"
        output += launch_template.format(**ln_dict)

        index+=1
        
    return output

def gen_partitioned_hash_join_aggregate_cpp(root, operator, operator_idx, operator_mapping, operator_idx_mapping):
    left_operator = operator_mapping[operator["left_input"]]
    left_operator_idx = operator_idx_mapping[operator["left_input"]]
    right_operator = operator_mapping[operator["right_input"]]
    right_operator_idx = operator_idx_mapping[operator["right_input"]]

    template_text = ""
    task_template = ""
    launcher_template = ""
    with open("partitioned_hash_join_aggregate.template", "r") as template:
        template_text = template.read()
    with open("read_partitioned_file_task.template", "r") as template:
        task_template = template.read()
    with open("read_partitioned_file_launch.template", "r") as template:
        launch_template = template.read()
    build_task_name = operator["stage_name"]+"BuildTask"
    probe_task_name = operator["stage_name"]+"ProbeTask"
    tasks = ""

    if "broadcast_inputs" in operator:
        index = 1
        for bcast_input in operator["broadcast_inputs"]:
            bcast_operator = operator_mapping[bcast_input[0]]
            bcast_operator_idx = operator_idx_mapping[bcast_input[0]]
            task_dict = {}
            task_dict["class_name"] = root["class_name"] 
            task_dict["stage_name"] = operator["stage_name"]
            task_dict["task_name"] = operator["stage_name"]+"BCastBuild{}".format(index)
            task_dict["in_parameters"] = ", ".join(["", "std::unordered_map<int64_t, std::vector<struct {record_type}>> *bcast_hash_map{index}".format(record_type=get_record_type(bcast_operator), index=index )]) 
            task_dict["intermediate_s3_bucket"]=root["intermediate_s3_bucket"]
            task_dict["additional_initialization"] = ""
            task_dict["row_processing"]=bcast_build_phase_process_record(bcast_operator, index)
            task_dict["in_rec_name"] = "left_rec"
            task_dict["in_max_encoding_size"]=get_max_encoding_size(bcast_operator, operator_mapping)
            task_dict["in_record_type"]=get_record_type(bcast_operator)
            task_dict["in_record_size"]=get_record_size(bcast_operator)
            task_dict["in_local_dicts"]=get_local_dicts(bcast_operator, operator_mapping)
            task_dict["in_populate_dicts"]=get_populate_dicts(bcast_operator, operator_mapping)
            task_dict["in_remap_dicts"]=get_remap_dicts(bcast_operator, operator_mapping, "left_rec")
            task_dict["in_operator_idx"]=bcast_operator_idx
            task_dict["uncompress"]=gen_uncompress(root)
            task_dict["post_row_processing"]=""
            tasks += task_template.format(**task_dict)
            index+=1
        
    task_dict = {}
    task_dict["class_name"] = root["class_name"] 
    task_dict["stage_name"] = operator["stage_name"]
    task_dict["task_name"] = build_task_name
    task_dict["in_parameters"] = ", ".join(["", "std::unordered_map<int64_t, std::vector<struct {left_record_type}>> *hash_map".format(left_record_type=get_record_type(left_operator))])
    task_dict["intermediate_s3_bucket"]=root["intermediate_s3_bucket"]
    task_dict["additional_initialization"] = ""
    task_dict["row_processing"]=join_build_phase_process_record(operator, operator_mapping, exists=operator["exists"] if "exists" in operator and operator["exists"] == "left" else "")
    task_dict["in_rec_name"] = "left_rec"
    task_dict["in_max_encoding_size"]=get_max_encoding_size(left_operator, operator_mapping)
    task_dict["in_record_type"]=get_record_type(left_operator)
    task_dict["in_record_size"]=get_record_size(left_operator)
    task_dict["in_local_dicts"]=get_local_dicts(left_operator, operator_mapping)
    task_dict["in_populate_dicts"]=get_populate_dicts(left_operator, operator_mapping)
    task_dict["in_remap_dicts"]=get_remap_dicts(left_operator, operator_mapping, "left_rec")
    task_dict["in_operator_idx"]=left_operator_idx
    task_dict["uncompress"]=gen_uncompress(root)
    task_dict["post_row_processing"]=""
    task_dict["operator_idx"]=operator_idx
    tasks += task_template.format(**task_dict)

    task_dict = {}
    task_dict["class_name"] = root["class_name"] 
    task_dict["stage_name"] = operator["stage_name"]
    task_dict["task_name"] = probe_task_name
    broadcast_params = []
    if "broadcast_inputs" in operator:
        index = 1
        for bcast_input in operator["broadcast_inputs"]:
            curr_param = "std::unordered_map<int64_t, std::vector<struct {record_type}>> *bcast_hash_map{index}".format(record_type=get_record_type(operator_mapping[bcast_input[0]]), index=index )
            broadcast_params.append(curr_param)
            index+=1
    task_dict["in_parameters"] = ", ".join(["", "std::unordered_map<int64_t, std::vector<struct {left_record_type}>> *hash_map".format(left_record_type=get_record_type(left_operator)), "std::unordered_map<struct {group_by_type}, struct {aggregate_type}> *out_agg_map".format(group_by_type="{}_group_by_record".format(get_request_name(operator).lower()), aggregate_type="{}_agg_record".format(get_request_name(operator).lower()))]+broadcast_params)
    task_dict["intermediate_s3_bucket"]=root["intermediate_s3_bucket"]
    task_dict["additional_initialization"] = "struct {right_record_type} out_rec;\n".format(right_record_type=get_record_type(right_operator))+"std::unordered_map<struct {group_by_type}, struct {aggregate_type}> local_agg_map;\n".format(group_by_type="{}_group_by_record".format(get_request_name(operator).lower()), aggregate_type="{}_agg_record".format(get_request_name(operator).lower()))

    task_dict["row_processing"]=aggregate_join_probe_phase_process_record(operator, operator_mapping, operator["exists"] if "exists" in operator else "") if "broadcast_inputs" not in operator else agg_join_probe_bcast_process_record(operator, operator_mapping, operator["exists"] if "exists" in operator else "")
    task_dict["in_rec_name"] = "right_rec"
    task_dict["in_max_encoding_size"]=get_max_encoding_size(right_operator, operator_mapping)
    task_dict["in_record_type"]=get_record_type(right_operator)
    task_dict["in_record_size"]=get_record_size(right_operator)
    task_dict["in_local_dicts"]=get_local_dicts(right_operator, operator_mapping)
    task_dict["in_populate_dicts"]=get_populate_dicts(right_operator, operator_mapping)
    task_dict["in_remap_dicts"]=get_remap_dicts(right_operator, operator_mapping, "right_rec")
    task_dict["in_operator_idx"]=right_operator_idx
    task_dict["post_row_processing"]=gen_reduce_agg(operator)
    task_dict["uncompress"]=gen_uncompress(root)
    task_dict["operator_idx"]=operator_idx

    tasks+= task_template.format(**task_dict)
    right_start_idx = "0"
    right_jump_idx = 1
    left_start_idx = "0"
    left_jump_idx = 1
    task_divisor = 1
    should_drop_left="true"
    left_cache_read_idx="taskNum"
    should_drop_right="true"
    right_cache_read_idx="taskNum"
    if "split_join" in operator:
        split_join = operator["split_join"]
        if split_join[0] == "right":
            right_jump_idx = split_join[1]
            right_start_idx = "taskNum%{}".format(split_join[1])
        else:
            left_jump_idx = split_join[1]
            left_start_idx = "taskNum%{}".format(split_join[1])
        task_divisor = split_join[1]
    if right_operator["type"] == "Combiner":
        right_start_idx = "taskNum/(inPartitions/{})".format(right_operator["partition_factor"])
        right_jump_idx = right_operator["partition_factor"]
    elif right_operator["type"] == "FinalAggregate":
        right_start_idx = "taskNum"
        right_jump_idx = "numRightFiles"
        should_drop_right = "false"
        right_cache_read_idx = "0"


    if left_operator["type"] == "Combiner":
        left_start_idx = "taskNum/(inPartitions/{})".format(left_operator["partition_factor"])
        left_jump_idx = left_operator["partition_factor"]
    elif left_operator["type"] == "FinalAggregate":
        left_start_idx = "taskNum"
        left_jump_idx = "numLeftFiles"
        should_drop_left = "false"
        left_cache_read_idx = "0"

    ln_dict = {}
    ln_dict["class_name"] = root["class_name"] 
    ln_dict["stage_name"] = operator["stage_name"]
    ln_dict["task_name"] = build_task_name
    ln_dict["tpool_name"] = "tpool1"
    ln_dict["numInFiles"] = "numLeftFiles"
    ln_dict["params"] = ", ".join(["", "hash_map.get()"])
    ln_dict["in_partition_id"] = "taskNum" if left_operator["type"] != "FinalAggregate" else "0"
    ln_dict["in_partitions"] = "inPartitions" if left_operator["type"] != "FinalAggregate" else "1"
    ln_dict["start_idx"] = left_start_idx
    ln_dict["jump_idx"] = left_jump_idx
    ln_dict["task_divisor"] = task_divisor
    ln_dict["in_operator_idx"] = left_operator_idx;
    ln_dict["should_drop"]= should_drop_left if "drop_left" not in operator else operator["drop_left"] 
    ln_dict["cache_read_idx"]=left_cache_read_idx
    build_task_launch = launch_template.format(**ln_dict)

    ln_dict = {}
    ln_dict["class_name"] = root["class_name"] 
    ln_dict["stage_name"] = operator["stage_name"]
    ln_dict["task_name"] = probe_task_name
    ln_dict["tpool_name"] = "tpool2"
    ln_dict["numInFiles"] = "numRightFiles"
    ln_dict["in_partition_id"] = "taskNum" if right_operator["type"] != "FinalAggregate" else "0"
    ln_dict["in_partitions"] = "inPartitions" if right_operator["type"] != "FinalAggregate" else "1"
    ln_dict["start_idx"] = right_start_idx
    ln_dict["jump_idx"] = right_jump_idx
    ln_dict["task_divisor"] = task_divisor
    extra_inputs = []
    index = 1
    if "broadcast_inputs" in operator:
        extra_inputs.append("bcast_hash_map{}.get()".format(index))
        index += 1

    ln_dict["params"] = ", ".join(["", "hash_map.get()", "agg_map.get()"]+extra_inputs)
    ln_dict["in_operator_idx"] = right_operator_idx
    ln_dict["should_drop"]=should_drop_right if "drop_right" not in operator else operator["drop_right"] 
    ln_dict["cache_read_idx"]=right_cache_read_idx
    probe_task_launch = launch_template.format(**ln_dict)

    tt_dict = {}
    tt_dict["class_name"] = root["class_name"] 
    tt_dict["stage_name"] = operator["stage_name"]
    tt_dict["operator_idx"]=operator_idx
    tt_dict["output_dictionaries"]=""
    tt_dict["tasks"]=tasks
    tt_dict["build_task_launch"]=build_task_launch
    tt_dict["probe_task_launch"]=probe_task_launch
    tt_dict["left_record_type"]=get_record_type(left_operator)
    tt_dict["group_by_type"]="{}_group_by_record".format(get_request_name(operator).lower())
    tt_dict["aggregate_type"]="{}_agg_record".format(get_request_name(operator).lower())
    tt_dict["output_type"]="{}_record".format(get_request_name(operator).lower())
    tt_dict["request_name"]=get_request_name(operator)
    tt_dict["operator_idx"]=operator_idx
    tt_dict["limit"] = "LONG_MAX" if "limit" not in operator else str(operator["limit"])+"L"
    tt_dict["having_condition"] = "true" if "having_condition" not in operator else operator["having_condition"]
    if "broadcast_inputs" in operator:
        tt_dict["broadcast_tables"] = gen_broadcast_inputs(root, operator, operator_mapping, operator_idx_mapping)
    else:
        tt_dict["broadcast_tables"] = ""
    tt_dict["convert_partitioned_data"]=gen_convert_output(root).format(operator_idx=operator_idx)
    if "partition_column" in operator:
        partition_column_hash = "out_rec."+operator["partition_column"]
        columns = operator["group_by_columns"]+operator["aggregate_columns"]
        index = [x[0] for x in columns].index(operator["partition_column"])
        if columns[index][1] == "STRING":
            partition_column_hash = "std::hash<std::string>()(std::string(out_rec.{}, {}))".format(columns[index][0], columns[index][2])


        tt_dict["set_partition"] = "partition_num = ({} % 91397) % outPartitions;".format(partition_column_hash)
        tt_dict["num_partitions"] = "outPartitions"
        tt_dict["out_partitions_param"] = "int outPartitions,"
    else:
        tt_dict["set_partition"] = ""
        tt_dict["num_partitions"] = 1;
        tt_dict["out_partitions_param"] = ""

    if "q16" in operator:
        tt_dict["count_distinct"] = """
        std::unique_ptr<std::unordered_map<struct {group_by_type}, struct {aggregate_type}>> agg_map2(new std::unordered_map<struct {group_by_type}, struct {aggregate_type}>);

        for (auto &it : *agg_map){{
          struct {group_by_type} agg_key;
          agg_key = it.first;
          agg_key.ps_suppkey = -1;
          auto &agg_rec  = (*agg_map2)[agg_key];
          agg_rec.supplier_cnt += 1;
        }}

        agg_map.swap(agg_map2);
        agg_map2.reset(nullptr);

        """.format(**tt_dict)

    else:
        tt_dict["count_distinct"] = ""

    out_text = template_text.format(**tt_dict)
 
    return out_text


def create_print_row(operator):
    output = "" 
    for column in operator["group_by_columns"]:
        if column[0] == "none":
            continue
        if (column[1] == "LONG" or column[1] == "DATE" or column[1] == "DOUBLE"):
            output += " "*4+"std::cout << rec.{} << \"|\";\n".format(column[0])

        elif(column[1] == "STRING"):
            output += " "*4+"std::cout << std::string(rec.{}, {}) << \"|\";\n".format(column[0], column[2])
    for column in operator["aggregate_columns"]:
        if (column[1] == "LONG" or column[1] == "DATE" or column[1] == "DOUBLE"):
            output += " "*4+"std::cout << rec.{} << \"|\";\n".format(column[0])

        elif(column[1] == "STRING"):
            output += " "*4+"std::cout << std::string(rec.{}, {}) << \"|\";\n".format(column[0], column[2])
    output+="std::cout << std::endl;\n"
    return output
            


def gen_final_aggregate_cpp(class_name, operator, operator_idx, operator_mapping, operator_idx_mapping):
    template_text = ""
    left_operator_idx = 0
    right_operator_idx = 0
    with open("final_aggregate.template", "r") as template:
        template_text = template.read()

    in_operator = operator_mapping[operator["in_operator"]]
    in_operator_idx = operator_idx_mapping[operator["in_operator"]]
    tt_dict = {}
    tt_dict["class_name"] = class_name
    tt_dict["stage_name"] = operator["stage_name"]
    tt_dict["in_operator_idx"]=in_operator_idx
    tt_dict["group_by_type"]="{}_group_by_record".format(get_request_name(in_operator).lower())
    tt_dict["aggregate_type"]="{}_agg_record".format(get_request_name(in_operator).lower())
    tt_dict["output_type"]="{}_record".format(get_request_name(in_operator).lower())
    tt_dict["in_request_name"]=get_request_name(in_operator)
    tt_dict["create_final_aggregate_record"]=create_final_agg_record(operator, in_operator)
    tt_dict["print_row"]=create_print_row(in_operator)
    limit = "LONG_MAX"
    if "limit" in operator:
        limit =str(operator["limit"])+"L"
    elif "limit" in in_operator:
        limit =str(in_operator["limit"])+"L"

    tt_dict["limit"] = limit
    tt_dict["finalize_out_rec"] = "" if "finalize_out_rec" not in operator else operator["finalize_out_rec"]
    tt_dict["having_condition"] = "true" if "having_condition" not in operator else operator["having_condition"]
    tt_dict["operator_idx"]=operator_idx
    tt_dict["should_drop"] = "true" if "should_drop" not in operator else operator["should_drop"]

    out_text = template_text.format(**tt_dict)
    return out_text

def gen_uncompress(root):
    if "compress" in root and root["compress"].lower() == "on":
        return """

        uint64_t uncompressed_len;
        snappy::GetUncompressedLength(data_buf, raw_len, &uncompressed_len);
        std::unique_ptr<char[]> buf_mgr(new char[uncompressed_len]);
        if(!snappy::RawUncompress(buf_mgr_raw.get(), raw_len, buf_mgr.get())){
          std::cout << "couldn't uncompress" << std::endl;
          exit(1);
        }
        total_len = uncompressed_len;
        buf_mgr_raw.reset();
        const char * buf = buf_mgr.get();
"""
    else:
        return """
        const char * buf = data_buf;
        total_len = raw_len;
        """
        
def gen_combiner_header(operator, operator_mapping):
    in_operator = operator_mapping[operator["in_operator"]]
    template_text = ""
    task_header_template = ""
    with open("combiner_header.template", "r") as template:
        template_text = template.read()
    with open("combine_task_header.template", "r") as template:
        task_header_template = template.read()
    task_name = operator["stage_name"]+"Task"
    broadcast_params = []
    out_text = ""
    out_text += task_header_template.format(task_name=task_name)
    out_text += template_text.format(stage_name=operator["stage_name"])

    return out_text

def gen_remap_combiner(operator, operator_mapping):
    out_text = ""
    in_operator = operator_mapping[operator["in_operator"]]
    found_dict = False
    for column in in_operator["out_columns"]:
        if column[1] == "STRING" and len(column) >=4 and column[3] == "DICT":
            found_dict = True
    if not found_dict:
        return ""
    out_text += " "*6+"for (int64_t rec_num = 0; rec_num < total_records; rec_num++){\n"
    out_text += " "*8+"struct {in_rec_name} &in_rec = *reinterpret_cast<{in_rec_name}*>(buf+rec_num*{in_record_size});\n".format(in_rec_name=get_record_type(in_operator), in_record_size=get_record_size(in_operator));
    out_text += get_remap_dicts(operator, operator_mapping, "in_rec")
    
    out_text += " "*6+"}\n"
    return out_text

def gen_combiner(root, operator, operator_idx, operator_mapping, operator_idx_mapping):
    in_operator = operator_mapping[operator["in_operator"]]
    in_operator_idx = operator_idx_mapping[operator["in_operator"]]

    template_text = ""
    task_template = ""
    launcher_template = ""
    with open("combiner.template", "r") as template:
        template_text = template.read()
    with open("combine_task.template", "r") as template:
        task_template = template.read()
    with open("combine_task_launch.template", "r") as template:
        launch_template = template.read()
    task_name = operator["stage_name"]+"Task"
    tasks = ""

    task_dict = {}
    task_dict["class_name"] = root["class_name"] 
    task_dict["stage_name"] = operator["stage_name"]
    task_dict["task_name"] = task_name
    task_dict["intermediate_s3_bucket"]=root["intermediate_s3_bucket"]
    task_dict["in_max_encoding_size"]=get_max_encoding_size(in_operator, operator_mapping)
    task_dict["in_record_size"]=get_record_size(in_operator)
    task_dict["in_local_dicts"]=get_local_dicts(in_operator, operator_mapping)
    task_dict["in_populate_dicts"]=get_populate_dicts(in_operator, operator_mapping)
    task_dict["in_remap_dicts"]=get_remap_dicts(in_operator, operator_mapping, "left_rec")
    task_dict["in_operator_idx"]=in_operator_idx
    task_dict["uncompress"]=gen_uncompress(root)
    task_dict["remap_records"]=gen_remap_combiner(operator, operator_mapping)
    tasks += task_template.format(**task_dict)

    ln_dict = {}
    ln_dict["class_name"] = root["class_name"] 
    ln_dict["stage_name"] = operator["stage_name"]
    ln_dict["task_name"] = task_name
    ln_dict["tpool_name"] = "tpool1"
    ln_dict["numInFiles"] = "numInFiles"
    ln_dict["in_partition_id"] = "taskNum"
    extra_inputs = []

    ln_dict["params"] = ", ".join(["", "hash_map.get()", "agg_map.get()"]+extra_inputs)
    task_launch = launch_template.format(**ln_dict)

    tt_dict = {}
    tt_dict["class_name"] = root["class_name"] 
    tt_dict["stage_name"] = operator["stage_name"]
    tt_dict["operator_idx"]=operator_idx
    tt_dict["output_dictionaries"]=gen_output_dictionaries(in_operator)
    tt_dict["tasks"]=tasks
    tt_dict["task_launch"]=task_launch
    tt_dict["request_name"]=get_request_name(operator)
    tt_dict["operator_idx"]=operator_idx
    tt_dict["file_factor"]=operator["file_factor"]
    tt_dict["partition_factor"]=operator["partition_factor"]

    out_text = template_text.format(**tt_dict)
    return out_text

def gen_sleep_header(root, operator):
    template_text = ""
    with open("sleep_header.template", "r") as template:
        template_text = template.read()
    tt_dict = {}
    tt_dict["class_name"] = root["class_name"] 
    tt_dict["stage_name"] = operator["stage_name"]
    return template_text.format(**tt_dict)

def gen_sleep_cpp(root, operator):
    template_text = ""
    with open("sleep.template", "r") as template:
        template_text = template.read()
    tt_dict = {}
    tt_dict["class_name"] = root["class_name"] 
    tt_dict["stage_name"] = operator["stage_name"]
    return template_text.format(**tt_dict)

def gen_skeleton(args):
    with open(args.in_file, "r") as in_file:
        parsed = json.load(in_file)
        common.update_query_file(parsed, "tmp_params")
        class_name = parsed["class_name"]
        operators = parsed["stages"]
        operator_idx = 0
        operator_mapping = {}
        operator_idx_mapping = {}
        for operator in operators:
            operator_mapping[operator["stage_name"]] = operator
            operator_idx_mapping[operator["stage_name"]] = operator_idx
            if operator["type"] in ["PartitionedHashJoinAggregate", "BaseTableScanAggregate"]:
                operator["out_columns"] = operator["aggregate_columns"]+operator["group_by_columns"]
            if operator["type"] in ["Combiner", "FinalAggregate"]:
                operator["out_columns"] = operator_mapping[operator["in_operator"]]
            operator_idx += 1
        request_types = gen_request_types(operators)
        request_switch  = gen_request_switch(operators, operator_mapping)
        structs = gen_structs(operators)
        parse_args = gen_parse_args(operators)
        dictionary_encodings = generate_dictionary_encodings_header(operators)
        operator_idx = 0
        function_declarations = ""
        stage_implementations = ""
        for operator in operators:
            if operator['type'] == "BaseTableScanPartition":
                function_declarations += gen_base_table_scan_partition_header(parsed, operator)
                stage_implementations += gen_base_table_scan_partition_cpp(parsed, operator, operator_idx)
            if operator['type'] == "BaseTableScanAggregate":
                function_declarations += gen_base_table_scan_aggregate_header(parsed, operator)
                stage_implementations += gen_base_table_scan_aggregate_cpp(parsed, operator, operator_idx)
            if operator['type'] == "PartitionedHashJoin":
                function_declarations += gen_partitioned_hash_join_header(operator, operator_mapping)
                stage_implementations += gen_partitioned_hash_join_cpp(parsed, operator, operator_idx, operator_mapping, operator_idx_mapping)
            if operator['type'] == "BroadcastJoinBaseTable":
                function_declarations += gen_broadcast_join_base_table_header(operator, operator_mapping)
                stage_implementations += gen_broadcast_join_base_table_cpp(parsed, operator, operator_idx, operator_mapping, operator_idx_mapping)
            if operator['type'] == "TwoBaseTableJoinPartition":
                function_declarations += gen_two_base_table_join_partition_header(operator, operator_mapping)
                stage_implementations += gen_two_base_table_join_partition_cpp(parsed, operator, operator_idx, operator_mapping, operator_idx_mapping)
            if operator['type'] == "PartitionedHashJoinAggregate":
                function_declarations += gen_partitioned_hash_join_aggregate_header(operator, operator_mapping)
                stage_implementations += gen_partitioned_hash_join_aggregate_cpp(parsed, operator, operator_idx, operator_mapping, operator_idx_mapping)
            if operator['type'] == "Combiner":
                function_declarations += gen_combiner_header(operator, operator_mapping)
                stage_implementations += gen_combiner(parsed, operator, operator_idx, operator_mapping, operator_idx_mapping)
            if operator['type'] == "FinalAggregate":
                function_declarations += gen_final_aggregate_header(class_name, operator, operator_mapping[operator["in_operator"]])
                stage_implementations += gen_final_aggregate_cpp(class_name, operator, operator_idx, operator_mapping, operator_idx_mapping)
            if operator['type'] == "Sleep":
                function_declarations += gen_sleep_header(parsed, operator)
                stage_implementations += gen_sleep_cpp(parsed, operator)
            operator_idx+=1
        with open("../src/include/{}.h".format(class_name), "w") as h_f, open("../src/{}.cpp".format(class_name), "w")as cpp_f:
            header_constants, cpp_constants = gen_constants(parsed["constants"])
            with open("header.template", "r") as h_template:
                template = h_template.read()
                h_f.write(template.format(class_name=class_name, function_declarations=function_declarations, constants=header_constants, structs=structs, request_types=request_types, dictionary_encodings=dictionary_encodings, hash_functions=gen_hash_functions(class_name, operators)))
            with open("cpp.template", "r") as cpp_template:
                template = cpp_template.read()
                cpp_f.write(template.format(class_name=class_name, stage_implementations=stage_implementations, pre_initialization=parsed["pre_initialization"], initialization=cpp_constants, request_type_switch=request_switch, parse_args=parse_args))


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("in_file", type=str)
    args = parser.parse_args()
    gen_skeleton(args);
