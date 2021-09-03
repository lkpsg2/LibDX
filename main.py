from __future__ import division
import redis
import time
import json


def match(r, features):
    candidates = {}
    str_score = {}
    count = 0
    for feature in features:
        if len(feature) > 5:
            count += 1
            str_score[feature] = {}
            for item in r.lrange(feature, 0, -1):
                # item[:-13] is the id for each binary file in database.
                if item[:-13] not in candidates:
                    candidates[item[:-13]] = set()
                    candidates[item[:-13]].add(feature)
                else:
                    candidates[item[:-13]].add(feature)
                # item[-12:] is the score for feature in this binary file.
                # it can be calculated using tf-idf
                str_score[feature][item[:-13]] = float(item[-12:])
    print 'string search operation times:', count
    return candidates, str_score


def filter_candidates(files2strs_redis, candidates, str_score):
    result = {}
    candidates_score = {}
    filter_threshold = 0.10
    print 'original candidates length: ', len(candidates)
    for candidate in candidates:
        score = 0
        for feature in set(candidates[candidate]):
            score += str_score[feature][candidate]
        if float(files2strs_redis.lrange(candidate, 0, 0)[0]) != 0:
            candidates_score[candidate] = score / float(
                files2strs_redis.lrange(candidate, 0, 0)[0])
        else:
            candidates_score[candidate] = 0
        if int(files2strs_redis.lrange(
                candidate, 1,
                1)[0]) > 20 and candidates_score[candidate] > filter_threshold:
            result[candidate] = candidates[candidate]
    return result, candidates_score


def sort_candidates(files2strs_redis, candidates, candidates_score):
    result = sorted(
        candidates.items(), key=lambda x: candidates_score[x[0]], reverse=True)
    return result


def get_meta_group_single_lib(points_x):
    meta_group = []
    start = points_x[0]
    end = points_x[0]
    points_len = len(points_x)
    for index, val in enumerate(points_x):
        if index + 1 == points_len:
            length = end - start + 1
            feature_num = length
            meta_group.append((start, end, length, feature_num))
            continue
        if points_x[index + 1] - val > 1:
            length = end - start + 1
            feature_num = length
            meta_group.append((start, end, length, feature_num))
            start = points_x[index + 1]
            end = start
        else:
            end = points_x[index + 1]
    return meta_group


def get_meta_groups(points_x_dict):
    meta_groups = {}
    for points_x in points_x_dict:
        meta_groups[points_x] = get_meta_group_single_lib(
            points_x_dict[points_x])
    return meta_groups


def get_merge_groups(meta_groups):
    result = {}
    for meta_group in meta_groups:
        result[meta_group] = get_merge_group(meta_groups[meta_group])
    return result


def get_merge_group(meta_group):
    mark_length = 10
    flag = 0
    result = []
    group = []
    for i in meta_group:
        if group == []:
            group = list(i)
            if i[2] >= mark_length:
                flag = 1
        else:
            if i[0] - group[1] > mark_length:
                if flag == 1:
                    result.append(tuple(group))
                    group = list(i)
                    if group[2] < mark_length:
                        flag = 0
                else:
                    group = list(i)
                    if group[2] >= mark_length:
                        flag = 1
            else:
                group[1] = i[1]
                group[2] = i[1] - group[0] + 1
                group[3] += i[3]
                if i[2] >= mark_length:
                    flag = 1
    if flag == 1:
        result.append(tuple(group))
    return result


def get_points_x_dict(sorted_candidates, features_detection):
    points_x_dict = {}
    for sorted_candidates_item in sorted_candidates:
        points_x = []
        points_x = [
            index for index, val in enumerate(features_detection)
            if val in sorted_candidates_item[1]
        ]
        points_x = sorted(set(points_x))
        if points_x == []:
            print sorted_candidates_item[0]
        points_x_dict[sorted_candidates_item[0]] = points_x
    return points_x_dict


def get_points_x_dict_from_candidates(
        files2strs_redis, sorted_candidates_items, features_detection):
    points_x_dict = get_points_x_dict(sorted_candidates_items,
                                      features_detection)
    return points_x_dict


def group_candidates(points_x_dict):
    meta_groups = get_meta_groups(points_x_dict)
    merge_groups = get_merge_groups(meta_groups)
    return merge_groups


def get_logic_block_points_x(points_x_dict, merged_groups):
    grouped_candidates_index = {}
    for group in merged_groups:
        full_index_in_groups = []
        for i in merged_groups[group]:
            full_index_in_groups += range(i[0], i[1] + 1)
        grouped_candidates_index[group] = list(
            set(full_index_in_groups).intersection(set(points_x_dict[group])))
    return grouped_candidates_index


def get_logic_block_candidates(grouped_candidates_index, features_detection):
    logic_block_candidates = {}
    for candidate in grouped_candidates_index:
        logic_block_candidates[candidate] = [
            features_detection[index]
            for index in grouped_candidates_index[candidate]
        ]
    return logic_block_candidates


def file_group_id_best(filtered_logic_block_candidates, candidates_score):
    file_groups = {}
    for i in filtered_logic_block_candidates:
        file_group_id = i[0:6]
        if file_group_id not in file_groups:
            file_groups[file_group_id] = [
                i, filtered_logic_block_candidates[i]
            ]
        else:
            if candidates_score[i] > candidates_score[file_groups[file_group_id]
                                                      [0]]:
                file_groups[file_group_id] = [
                    i, filtered_logic_block_candidates[i]
                ]
            else:
                if candidates_score[i] == candidates_score[file_groups[file_group_id][0]] and len(
                        filtered_logic_block_candidates[i]
                ) > len(filtered_logic_block_candidates[file_groups[file_group_id]
                                                        [0]]):
                    file_groups[file_group_id] = [
                        i, filtered_logic_block_candidates[i]
                    ]
    grouped_candidates = {}
    for i in file_groups:
        grouped_candidates[file_groups[i][0]] = file_groups[i][1]
    return grouped_candidates


def file_group_best(filtered_logic_block_candidates, candidates_score):
    filtered_logic_block_candidates = file_group_id_best(
        filtered_logic_block_candidates, candidates_score)
    file_groups = {}
    for i in filtered_logic_block_candidates:
        file_group_id = i[14:21]
        if file_group_id not in file_groups:
            file_groups[file_group_id] = [
                i, filtered_logic_block_candidates[i]
            ]
        else:
            if candidates_score[i] > candidates_score[file_groups[file_group_id]
                                                      [0]]:
                file_groups[file_group_id] = [
                    i, filtered_logic_block_candidates[i]
                ]
            else:
                if candidates_score[i] == candidates_score[file_groups[file_group_id][0]] and len(
                        filtered_logic_block_candidates[i]
                ) > len(filtered_logic_block_candidates[file_groups[file_group_id]
                                                        [0]]):
                    file_groups[file_group_id] = [
                        i, filtered_logic_block_candidates[i]
                    ]
    grouped_candidates = {}
    for i in file_groups:
        grouped_candidates[file_groups[i][0]] = file_groups[i][1]
    return grouped_candidates


def logic_block_group_best(candidates_file_group_best,
                           grouped_candidates_index, candidates_score):
    candidates_file_group_best_index = {}
    for candidate in candidates_file_group_best:
        candidates_file_group_best_index[candidate] = grouped_candidates_index[
            candidate]
    candidates_file_group_best_index = sorted(
        candidates_file_group_best_index.items(), key=lambda x: len(x[1]))
    result = {}
    compared_list = []
    compare_groups = []
    compare_group = set()
    for index, val in enumerate(candidates_file_group_best_index):
        if val[0] in compared_list:
            continue

        compared_list.append(val[0])
        compare_group.add(val[0])
        for sub_index, sub_val in enumerate(
                candidates_file_group_best_index[index:]):
            if compare2list(val[1], sub_val[1]):
                compare_group.add(sub_val[0])
                compared_list.append(sub_val[0])
            else:
                if len(sub_val[1]) > 1.5 * len(val[1]):
                    break
        compare_groups.append(compare_group)
        compare_group = set()
    # print compare_groups
    for compare_group in compare_groups:
        max_score = 0
        for candidate in compare_group:
            if candidates_score[candidate] > max_score:
                compare_group_best = candidate
                max_score = candidates_score[candidate]
        result[compare_group_best] = candidates_score[compare_group_best]
    return result


def compare2list(list0, list1):
    inter = list(set(list0).intersection(set(list1)))
    if len(inter) / float(len(list0)) > 0.8 and len(inter) / float(
            len(list1)) > 0.8:
        return True
    else:
        return False


def detect_single_file(strs2files_redis,
                       files2strs_redis,
                       test_file,
                       save_file=None):
    start_all = time.clock()

    candidates = {}

    start = time.clock()
    features_detection = test_file['features']

    if features_detection:
        match_info = {}
        match_info['features_num'] = len(features_detection)

        start = time.clock()
        candidates, str_score = match(strs2files_redis, features_detection)
        print 'get original candidates'
        candidates, candidates_score = filter_candidates(
            files2strs_redis, candidates, str_score)
        sort_candidates_items = sort_candidates(files2strs_redis, candidates,
                                                candidates_score)
        print 'get sorted candidates items'

        points_x_dict = get_points_x_dict_from_candidates(
            files2strs_redis, sort_candidates_items, features_detection)
        merged_groups = group_candidates(points_x_dict)
        print 'merged groups'

        grouped_candidates_index = get_logic_block_points_x(
            points_x_dict, merged_groups)
        print 'grouped_candidates_index length:', len(grouped_candidates_index)

        logic_block_candidates = get_logic_block_candidates(
            grouped_candidates_index, features_detection)
        print 'get logic candidates:', len(logic_block_candidates)

        filtered_logic_block_candidates, candidates_score = filter_candidates(
            files2strs_redis, logic_block_candidates, str_score)
        print 'len filtered logic block candidates', len(
            filtered_logic_block_candidates)

        candidates_file_group_best = file_group_best(
            filtered_logic_block_candidates, candidates_score)
        print 'grouped logic block candidates', len(candidates_file_group_best)

        match_result = logic_block_group_best(candidates_file_group_best,
                                              grouped_candidates_index,
                                              candidates_score)

        match_info['match_time'] = time.clock() - start
        print 'match_time:', match_info['match_time']
        match_info['match_result'] = match_result

    detection_time = time.clock() - start_all
    report_info['detection_time'] = detection_time
    print 'total detection time:', detection_time
    if save_file:
        with open(save_file, 'w') as s_file:
            json.dump(match_info, s_file)
    print '\n', 'detection over'


def get_fuzzy_name(name):
    name = name.lower()
    if name.startswith('lib'):
        name = name[3:]
    name = name.split('.')[0]
    name = name.rstrip('0123456789_')
    return name


def fuzzy_name(name1, name2):
    if get_fuzzy_name(name1) == get_fuzzy_name(name2):
        return True
    else:
        print('please use features to detect')
        return False


if __name__ == '__main__':
    # r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    strs2files_redis = redis.Redis(
        host='localhost', port=6379, decode_responses=True, db=3)
    files2strs_redis = redis.Redis(
        host='localhost', port=6379, decode_responses=True, db=4)

    name1 = 'detection_target_filename'
    name2 = 'lib)_binary_filename'
    # name comparison, you can search the fuzzy name of detection target after creating the table in redis.
    fuzzy_name(name1, name2)

    # test_file is a dict that save the features of detection target.
    test_file = {'features': ['feature1', 'feature2']}
    save_file = ''
    error_projects = []
    detect_single_file(strs2files_redis, files2strs_redis, test_file,
                       save_file)

    # data_format:
    # strs2files_redis:
    #   redis key-value 格式。 key为 feature， valua 为 该feature所在的库文件的列表。
    #   {key:[libfile1_score, libfile2_score]}
    #   其中每一个 item， item[:-13] 为该文件的唯一标识符，item[-12:] 为该特征对于该文件的分数。
    #   分数可由tfidf算法计算得到，也可直接认为1.
    #
    # files2strs_redis：
    #   redis 数据库中，由文件到文件所包含的特征的正向关系表。
    #   key 为数据库中文件的唯一标识符。value为列表。该列表第一项为所有该文件包含的所有特征分数之和，
    #   第二项为特征个数，后面分别存储特征内容。
    #   {key:[score_sum, feature_num, feature1]}
