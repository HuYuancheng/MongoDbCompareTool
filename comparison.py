# -*- coding: utf-8 -*-

"""
Module Description:
Date: 2023/4/13
Author: HuYuanCheng
"""
import ast
import contextvars
import getopt
import hashlib
import json
import sys
import time
import os
import itertools
import motor.motor_asyncio
import asyncio

_base_dir = os.getcwd()

COMPARISION_SAMPLES = "sample_list"
COMPARISION_MODE = "comparison_mode"
COMPARE_DBS = "compare_dbs"
COMPARE_COLLS = "compare_colls"

process_id = contextvars.ContextVar('Id of process')


class AsyncMongoCluster:
    conn = None
    url = ""

    def __init__(self, url):
        self.url = url

    def connect(self):
        self.conn = motor.motor_asyncio.AsyncIOMotorClient(self.url)

    def close(self):
        self.conn.close()


class AsyncDbCompare:
    def __init__(self, configure):
        self.src = None
        self.dst = None
        self.log = None
        self.error_log = None
        self.src_doc_count = 0
        self.dst_doc_count = 0
        self.process_document_res = True
        self.configure = configure
        file_start = configure['sample_start_idx']
        file_end = configure['sample_start_idx'] + configure["sample_count"]
        self.log_file_path = _base_dir + f'\\cmp_export\\cmp_log_{file_start}_{file_end}.txt'
        self.error_log_file_path = _base_dir + f'\\cmp_export\\cmp_diff_log_{file_start}_{file_end}.txt'
        self.process_count = 0
        self.process_time = 0
        self.total_count = 0
        self.task_count = 0
        self.tasks = []

    def log_info(self, message):
        msg = "INFO  [%s] %s " % (time.strftime('%Y-%m-%d %H:%M:%S'), message)
        print(msg)
        self.log.write(msg + '\n')
        self.log.flush()

    def log_error(self, message):
        msg = "ERROR [%s] %s " % (time.strftime('%Y-%m-%d %H:%M:%S'), message)
        print(msg)
        if not self.error_log:
            os.makedirs(os.path.dirname(self.error_log_file_path), exist_ok=True)
            self.error_log = open(self.error_log_file_path, 'a+')
        self.error_log.write(msg + '\n')
        self.error_log.flush()

    async def check(self):
        check_db_names = self.configure[COMPARE_DBS]
        for db in check_db_names:
            src_db = self.src.conn[db]
            dst_db = self.dst.conn[db]

            check_colls = self.configure[COMPARE_COLLS]
            for coll in check_colls:
                coll_start = time.time()
                src_coll = src_db[coll]
                dst_coll = dst_db[coll]
                self.src_doc_count = await src_coll.estimated_document_count()
                coll_end = time.time()
                print("Collection time: {}".format(coll_end - coll_start))

                before = time.time()
                compare_result = await self.data_comparison(src_coll, dst_coll, self.configure[COMPARISION_MODE])
                if not compare_result:
                    self.log_error("DIFF => collection [%s] data comparison not equals" % coll)
                    return False
                else:
                    self.log_info("PASS => collection [%s] data data comparison exactly equals" % coll)
                after = time.time()
                self.log_info("collection comparison runtime: {}, avg time: {}".format(after - before,
                                                                                       self.process_time / self.task_count))

        return True

    async def data_comparison(self, src_coll, dst_coll, mode):
        samples = self.configure.get(COMPARISION_SAMPLES, [])
        if mode == "sample":
            sample_count = len(samples)
            count = sample_count \
                if sample_count <= self.src_doc_count \
                else self.src_doc_count
        else:
            count = self.src_doc_count

        if count == 0:
            return True

        self.total_count = count
        self.log_info("Process Count: {}".format(count))

        batch = self.configure.get('query_batch', 20)

        tasks = []
        start_idx = 0
        samples = [int(x) for x in samples]
        while count > 0:
            end_idx = start_idx + batch if batch < count else start_idx + count
            values = samples[start_idx: end_idx]
            tasks.append(self.process_document(src_coll, dst_coll, values, start_idx))
            count -= batch
            start_idx = end_idx

        self.tasks = tasks
        self.task_count = len(tasks)
        self.log_info("create tasks count: {}".format(len(tasks)))

        sub_tasks = []
        max_task_count = self.configure['task_count']
        for i in range(max_task_count):
            sub_tasks.append(asyncio.create_task(self.sub_future(i)))

        await asyncio.gather(*sub_tasks)

        return self.process_document_res

    async def sub_future(self, index):
        print("[{}] tasks: {}".format(index, len(self.tasks)))
        while len(self.tasks) > 0:
            await asyncio.create_task(self.tasks.pop())
            print("[{}] tasks: {}".format(index, len(self.tasks)))

    async def get_src_cursor_data(self, src_coll, values, start_idx):
        start4 = time.time()
        src_md5 = {}

        src_cursor = src_coll.find({"_id": {"$in": values}})
        # docs = await src_cursor.to_list(length=len(values))
        # if not docs:
        #     self.log_error("src find result empty, values: {}".format(values))
        #     return

        if not isinstance(src_cursor, list):
            while src_cursor.alive:
                try:
                    doc = await src_cursor.next()
                except Exception as e:
                    self.log_error("src next failed! values: {}".format(values))
                    continue
                await asyncio.sleep(0)
                md5 = hashlib.md5(str(doc).replace(': ', ':').replace(', ', ',').encode('utf - 8')).hexdigest()
                src_md5[doc['_id']] = md5
        end4 = time.time()

        self.log_info("[{}] get src cursor time: {}".format(start_idx, end4 - start4))

        return src_md5

    async def get_dst_cursor_data(self, dst_coll, values, start_idx):
        start5 = time.time()
        dst_md5 = {}

        dst_cursor = dst_coll.find({"_id": {"$in": values}})
        # docs = await dst_cursor.to_list(length=len(values))
        # if not docs:
        #     self.log_error("dst find result empty, values: {}".format(values))
        #     return

        if not isinstance(dst_cursor, list):
            while dst_cursor.alive:
                try:
                    dst = await dst_cursor.next()
                except Exception as e:
                    self.log_error("dst next failed!")
                    continue
                await asyncio.sleep(0)
                md5 = hashlib.md5(str(dst).replace(': ', ':').replace(', ', ',').encode('utf - 8')).hexdigest()
                dst_md5[dst['_id']] = md5
        end5 = time.time()

        self.log_info("[{}] get dst cursor time: {}".format(start_idx, end5 - start5))
        return dst_md5

    async def process_document(self, src_coll, dst_coll, values, start_idx):
        process_id.set(start_idx)

        task1 = asyncio.create_task(self.get_src_cursor_data(src_coll, values, start_idx))
        task2 = asyncio.create_task(self.get_dst_cursor_data(dst_coll, values, start_idx))

        start4 = time.time()
        src_docs = await task1
        dst_docs = await task2
        end4 = time.time()

        if not src_docs or not dst_docs:
            self.process_document_res = False
            return False

        if len(src_docs) != len(dst_docs):
            miss_ids = [i for i in list(src_docs.keys()) if i not in list(dst_docs.keys())]
            self.log_error("DIFF => src docs count: {} != dst docs count: {}, missing ids: {}".format(len(src_docs), len(dst_docs), miss_ids))

        for id, doc in src_docs.items():
            migrated = dst_docs.get(id, None)

            is_diff = doc != migrated
            if is_diff:
                self.log_error("DIFF => _id: {}".format(id))
                self.process_document_res = False

        end6 = time.time()
        self.log_info(
            "[{}] process {} docs total time :{}, get src and dst data time :{}".format(
                process_id.get(), len(values), end6 - start4, end4 - start4))
        self.process_time += end6 - start4
        self.process_count += len(values)
        self.log_info("-------------------------Process Progress: {}/{}----------------------------".format(
            self.process_count, self.total_count))
        return True

    async def do_compare(self):
        start = time.time()
        src_url, dst_url = self.configure["src_url"], self.configure["dst_url"]

        if len(src_url) == 0 or len(dst_url) == 0:
            return False

        if not src_url.startswith('mongodb:') or not dst_url.startswith('mongodb:'):
            self.log_error("ERROR => invalid mongodb url")
        try:
            self.src, self.dst = AsyncMongoCluster(src_url), AsyncMongoCluster(dst_url)
            self.src.connect()
            self.dst.connect()
        except Exception as e:
            self.log_error("create mongo connection failed {} | {}, msg: {}".format(src_url, dst_url, e))

        os.makedirs(os.path.dirname(self.log_file_path), exist_ok=True)
        self.log = open(self.log_file_path, "a+")

        self.log_info("==============================================")
        self.log_info("Configuration %s" % self.configure)

        # 开始比对
        result = False
        try:
            result = await self.check()
        except Exception as e:
            self.log_error("check failed {}" .format(e))

        end = time.time()
        self.log_info("runtime {}s".format(end - start))
        if result:
            self.log_info("SUCCESS")
        else:
            self.log_error("FAIL")

    def quit(self):
        self.src.close()
        self.dst.close()
        self.log.close()
        if self.error_log:
            self.error_log.close()


class AsyncDbWrite:
    def __init__(self, configure):
        self.src = None
        self.log = None
        self.error_log = None
        self.src_doc_count = 0
        self.configure = configure
        self.file_start = configure['sample_start_idx']
        self.file_end = configure['sample_start_idx'] + configure["sample_count"]
        self.coll_file_path = _base_dir + '\\write_export\\{}_{}_{}.txt'
        self.log_file_path = _base_dir + '\\write_export\\write_log_{}_{}.txt'.format(self.file_start, self.file_end)
        self.error_log_file_path = _base_dir + '\\write_export\\write_error_log_{}_{}.txt'.format(self.file_start,
                                                                                                  self.file_end)
        self.process_count = 0
        self.process_time = 0
        self.total_count = 0
        self.task_count = 0
        self.tasks = []
        self.src_docs = {}

    def log_info(self, message):
        msg = "INFO  [%s] %s " % (time.strftime('%Y-%m-%d %H:%M:%S'), message)
        print(msg)
        self.log.write(msg + '\n')
        self.log.flush()

    def log_error(self, message):
        msg = "ERROR [%s] %s " % (time.strftime('%Y-%m-%d %H:%M:%S'), message)
        print(msg)
        if not self.error_log:
            os.makedirs(os.path.dirname(self.error_log_file_path), exist_ok=True)
            self.error_log = open(self.error_log_file_path, 'a+')
        self.error_log.write(msg + '\n')
        self.error_log.flush()

    async def check_and_write(self):
        check_db_names = self.configure[COMPARE_DBS]
        for db in check_db_names:
            src_db = self.src.conn[db]
            check_colls = self.configure[COMPARE_COLLS]
            for coll in check_colls:
                coll_start = time.time()
                src_coll = src_db[coll]
                self.src_doc_count = await src_coll.estimated_document_count()
                coll_end = time.time()
                print("Collection time: {}".format(coll_end - coll_start))

                before = time.time()
                await self.data_write(coll, src_coll, self.configure[COMPARISION_MODE])
                after = time.time()
                self.log_info("collection write runtime: {}, avg time: {}".format(after - before,
                                                                                  self.process_time / self.task_count))

        return True

    async def data_write(self, coll_name, src_coll, mode):
        samples = self.configure.get(COMPARISION_SAMPLES, [])
        if mode == "sample":
            sample_count = len(samples)
            count = sample_count \
                if sample_count <= self.src_doc_count \
                else self.src_doc_count
        else:
            count = self.src_doc_count

        if count == 0:
            return True

        self.total_count = count
        self.log_info("Process Count: {}".format(count))

        batch = self.configure.get('query_batch', 20)

        tasks = []
        start_idx = 0
        samples = [int(x) for x in samples]
        while count > 0:
            end_idx = start_idx + batch if batch < count else start_idx + count
            values = samples[start_idx: end_idx]
            tasks.append(self.write_sample_document(src_coll, values, start_idx))
            count -= batch
            start_idx = end_idx

        self.task_count = len(tasks)
        self.tasks = tasks
        self.log_info("create tasks count: {}".format(len(tasks)))

        sub_tasks = []
        max_task_count = self.configure['task_count']
        for i in range(max_task_count):
            sub_tasks.append(asyncio.create_task(self.sub_future(i)))

        await asyncio.gather(*sub_tasks)

        start6 = time.time()
        directory = self.coll_file_path.format(coll_name, self.file_start, self.file_end)
        os.makedirs(os.path.dirname(directory), exist_ok=True)
        with open(directory, "a+", encoding='UTF-8') as w_file:
            w_file.write(json.dumps(self.src_docs))
        end6 = time.time()
        self.log_info("write file time: {}".format(end6 - start6))

    async def sub_future(self, index):
        print("[{}] tasks: {}".format(index, len(self.tasks)))
        while len(self.tasks) > 0:
            await asyncio.create_task(self.tasks.pop())
            print("[{}] tasks: {}".format(index, len(self.tasks)))

    async def write_sample_document(self, src_coll, values, start_idx):
        process_id.set(start_idx)

        start0 = time.time()

        src_docs = {}
        start3 = time.time()
        src_cursor = src_coll.find({"_id": {"$in": values}})
        if not isinstance(src_cursor, list):
            while src_cursor.alive:
                doc = await src_cursor.next()
                md5 = hashlib.md5(str(doc).replace(': ', ':').replace(', ', ',').encode('utf - 8')).hexdigest()
                src_docs[doc['_id']] = md5

        end3 = time.time()

        self.src_docs.update(src_docs)

        end0 = time.time()
        self.log_info(
            "[{}] process {} docs total time :{}, get src data time :{}".format(
                process_id.get(), len(values), end0 - start0,
                end3 - start3))
        self.process_time += end0 - start0
        self.process_count += len(values)
        self.log_info("-------------------------Process Progress: {}/{}----------------------------".format(
            self.process_count, self.total_count))

    async def do_write(self):
        start = time.time()
        src_url = self.configure["src_url"]

        if len(src_url) == 0:
            return False

        if not src_url.startswith('mongodb:'):
            self.log_error("ERROR => invalid mongodb url")
        try:
            self.src = AsyncMongoCluster(src_url)
            self.src.connect()
        except Exception as e:
            self.log_error("create mongo connection failed {}, msg: {}".format(src_url, e))

        os.makedirs(os.path.dirname(self.log_file_path), exist_ok=True)
        self.log = open(self.log_file_path, "a+")
        self.log_info("===============================================")
        self.log_info("Configuration %s" % self.configure)

        # 开始写入
        result = await self.check_and_write()
        end = time.time()
        self.log_info("runtime {}s".format(end - start))
        if result:
            print("SUCCESS")
            self.log_info("SUCCESS")
        else:
            print("FAIL")
            self.log_error("FAIL")

    def quit(self):
        self.src.close()
        self.log.close()
        if self.error_log:
            self.error_log.close()


class AsyncDbLoadCompare:
    def __init__(self, configure):
        self.src = None
        self.dst = None
        self.log = None
        self.error_log = None
        self.process_document_res = True
        self.dst_doc_count = 0
        self.configure = configure
        self.file_start = configure['sample_start_idx']
        self.file_end = configure['sample_start_idx'] + configure["sample_count"]
        self.coll_file_path = _base_dir + '\\write_export\\{}_{}_{}.txt'
        self.log_file_path = _base_dir + '\\load_export\\load_log_{}_{}.txt'.format(self.file_start, self.file_end)
        self.error_log_file_path = _base_dir + '\\load_export\\load_error_log_{}_{}.txt'.format(self.file_start,
                                                                                                self.file_end)
        self.process_count = 0
        self.process_time = 0
        self.total_count = 0
        self.task_count = 0
        self.tasks = []

    def log_info(self, message):
        msg = "INFO  [%s] %s " % (time.strftime('%Y-%m-%d %H:%M:%S'), message)
        print(msg)
        self.log.write(msg + '\n')
        self.log.flush()

    def log_error(self, message):
        msg = "ERROR [%s] %s " % (time.strftime('%Y-%m-%d %H:%M:%S'), message)
        print(msg)
        if not self.error_log:
            os.makedirs(os.path.dirname(self.error_log_file_path), exist_ok=True)
            self.error_log = open(self.error_log_file_path, 'a+')

        self.error_log.write(msg + '\n')
        self.error_log.flush()

    async def load_and_compare(self):
        check_db_names = self.configure[COMPARE_DBS]
        for db in check_db_names:
            src_db = self.src.conn[db]
            dst_db = self.dst.conn[db]
            check_colls = self.configure[COMPARE_COLLS]
            for coll in check_colls:
                coll_start = time.time()
                src_coll = src_db[coll]
                dst_coll = dst_db[coll]
                self.dst_doc_count = await dst_coll.estimated_document_count()
                coll_end = time.time()
                print("Collection time: {}".format(coll_end - coll_start))

                before = time.time()
                compare_result = await self.data_load_compare(coll, dst_coll, src_coll,
                                                              self.configure[COMPARISION_MODE])
                if not compare_result:
                    self.log_error("DIFF => collection [%s] data comparison not equals" % coll)
                    return False
                else:
                    self.log_info("PASS => collection [%s] data data comparison exactly equals" % coll)
                after = time.time()
                self.log_info("collection load and comparison runtime: {}, avg time: {}".format(after - before,
                                                                                                self.process_time / self.task_count))

        return True

    async def data_load_compare(self, coll_name, dst_coll, src_coll, mode):
        samples = self.configure.get(COMPARISION_SAMPLES, [])
        if mode == "sample":
            sample_count = len(samples)
            count = sample_count \
                if sample_count <= self.dst_doc_count \
                else self.dst_doc_count
        else:
            count = self.dst_doc_count

        if count == 0:
            return True

        self.total_count = count
        self.log_info("Process Count: {}".format(count))

        batch = self.configure.get('query_batch', 20)

        src_docs = {}
        directory = self.coll_file_path.format(coll_name, self.file_start, self.file_end)
        if not os.path.exists(directory):
            self.log_error("ERROR => No such log or directory: {}".format(directory))
            return False

        start0 = time.time()
        with open(directory, "r", encoding='UTF-8') as c_f:
            l = c_f.read()
            l = l.replace("}{", ",")
            src_docs.update(ast.literal_eval(l.strip()))
        end0 = time.time()
        self.log_info("load file time: {}, src docs count: {}".format(end0 - start0, len(src_docs)))

        tasks = []
        start_idx = 0
        samples = [int(x) for x in samples]
        while count > 0:
            end_idx = start_idx + batch if batch < count else start_idx + count
            values = samples[start_idx: end_idx]
            tasks.append(self.compare_sample_document(src_docs, dst_coll, src_coll, values, start_idx))
            count -= batch
            start_idx = end_idx

        self.tasks = tasks
        self.task_count = len(tasks)
        self.log_info("create tasks count: {}".format(len(tasks)))

        sub_tasks = []
        max_task_count = self.configure['task_count']
        for i in range(max_task_count):
            sub_tasks.append(asyncio.create_task(self.sub_future(i)))

        await asyncio.gather(*sub_tasks)

        return self.process_document_res

    async def sub_future(self, index):
        print("[{}] tasks: {}".format(index, len(self.tasks)))
        while len(self.tasks) > 0:
            await asyncio.create_task(self.tasks.pop())
            print("[{}] tasks: {}".format(index, len(self.tasks)))

    async def compare_sample_document(self, src_docs, dst_coll, src_coll, values, start_idx):
        process_id.set(start_idx)

        start3 = time.time()
        dst_cursor = dst_coll.find({"_id": {"$in": values}})
        dst_docs = {}
        while dst_cursor.alive:
            migrated = await dst_cursor.next()
            migrated_md5 = hashlib.md5(
                str(migrated).replace(': ', ':').replace(', ', ',').encode('utf - 8')).hexdigest()
            dst_docs[migrated['_id']] = migrated_md5
        end3 = time.time()

        for id, dst_md5 in dst_docs.items():
            doc_md5 = src_docs.get(str(id), 0)
            if doc_md5 == 0:
                self.log_error("[{}] {} not in src_docs".format(process_id.get(), id))
                doc_res = await src_coll.find_one({"_id": id})
                doc_md5 = hashlib.md5(
                    str(doc_res).replace(': ', ':').replace(', ', ',').encode('utf - 8')).hexdigest()

            is_diff = doc_md5 != dst_md5
            if is_diff:
                self.log_error("DIFF => _id: {}".format(id))
                self.process_document_res = False

        end6 = time.time()
        self.log_info(
            "[{}] process {} docs total time :{}, get dst data time :{}".format(
                process_id.get(), len(values), end6 - start3,
                end3 - start3))
        self.process_time += end6 - start3
        self.process_count += len(values)
        self.log_info("-------------------------Process Progress: {}/{}----------------------------".format(
            self.process_count, self.total_count))

        return True

    async def do_load_compare(self):
        start = time.time()
        src_url, dst_url = self.configure["src_url"], self.configure["dst_url"]

        if len(src_url) == 0 or len(dst_url) == 0:
            return False

        if not src_url.startswith('mongodb:') or not dst_url.startswith('mongodb:'):
            self.log_error("ERROR => invalid mongodb url")
        try:
            self.src, self.dst = AsyncMongoCluster(src_url), AsyncMongoCluster(dst_url)
            self.src.connect()
            self.dst.connect()
        except Exception as e:
            self.log_error("create mongo connection failed {}, msg: {}" .format(dst_url, e))

        os.makedirs(os.path.dirname(self.log_file_path), exist_ok=True)
        self.log = open(self.log_file_path, "a+")
        self.log_info("============================================")
        self.log_info("Configuration %s" % self.configure)

        # 开始对比
        result = await self.load_and_compare()
        end = time.time()
        self.log_info("runtime {}s".format(end - start))
        if result:
            self.log_info("SUCCESS")
        else:
            self.log_error("FAIL")

    def quit(self):
        self.src.close()
        self.dst.close()
        self.log.close()
        if self.error_log:
            self.error_log.close()


async def compare(configure):
    cmp = AsyncDbCompare(configure)
    await cmp.do_compare()
    cmp.quit()
    # input("Press Any Key...")


async def write(configure):
    w = AsyncDbWrite(configure)
    await w.do_write()
    w.quit()
    # input("Press Any Key...")


async def load_and_compare(configure):
    l = AsyncDbLoadCompare(configure)
    await l.do_load_compare()
    l.quit()
    # input("Press Any Key...")


def usage():
    print("usage")
    print(
        '|------------------------------------------------------------------------------------------------------------------|')
    print(
        '| Like : python3 comparison.py -m 2 -f compare_conf.json -p 1 -i 0 -c 200 -b 20 -t 3')
    print(
        '| Or: python3 comparison.py --mode=2 --cfg_file=compare_conf.json --period=1 --start_idx=0 --count=200 --batch=20 --task_count=3')
    print(
        '|------------------------------------------------------------------------------------------------------------------|')
    exit(0)


if __name__ == '__main__':
    opts, args = getopt.getopt(sys.argv[1:], "hm:p:f:i:c:b:t:",
                               ["help", "mode=", 'period=', 'cfg_file=', 'start_idx=', 'count=', 'batch=', 'task_count='])

    cfg_file = "compare_conf.json"
    sample_start_idx = -1
    sample_count = -1
    batch = -1
    task_count = -1
    mode = -1
    period = -1
    for key, value in opts:
        if key in ("-h", "--help"):
            usage()
        if key in ("-m", "--mode"):
            mode = int(value)
        if key in ("-p", "--period"):
            period = int(value)
        if key in ("-f", "--cfg_file"):
            cfg_file = value
        if key in ("-i", "--start_idx"):
            sample_start_idx = int(value)
        if key in ("-c", "--count"):
            sample_count = int(value)
        if key in ("-b", "--batch"):
            batch = int(value)
        if key in ("-t", "--task_count"):
            task_count = int(value)

    with open(cfg_file, 'r') as cmp_cfg:
        configure = json.load(cmp_cfg)

    if sample_start_idx < 0:
        sample_start_idx = configure['sample_start_idx']
    else:
        configure['sample_start_idx'] = sample_start_idx

    if sample_count <= 0:
        sample_count = configure["sample_count"]
    else:
        configure["sample_count"] = sample_count

    if batch > 0:
        configure['query_batch'] = batch

    if task_count > 0:
        configure['task_count'] = task_count

    configure["comparison_mode"] = "sample"

    end = sample_start_idx + sample_count
    sample_list = []
    with open(configure["sample_file_name"], 'r', encoding='utf-8', errors='ignore') as f:
        for line in itertools.islice(f, sample_start_idx, end, 1):
            line = line.strip('\n')
            sample_list.append(line)

    configure["sample_list"] = sample_list
    if len(sample_list) == 0:
        exit(-1)

    if mode == 1:
        # 同步对比
        asyncio.run(compare(configure))
    elif mode == 2:
        if period == 1:
            # 写入文件
            asyncio.run(write(configure))
        elif period == 2:
            # 加载文件对比
            asyncio.run(load_and_compare(configure))
