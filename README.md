# MongoDbCompareTool
mongodb数据库迁移后需要对比src与dst数据一致性，该项目支持两种比对模式：(1)同时拉取数据进行对比;(2)先拉取src数据写入文件，再拉取dst数据与src文件对比。分步模式下可更好地利用迁移时间
py文件运行需要部署python3环境，可通过pyinstaller打包成exe运行

## 命令选项
|短选项	|长选项		    |含义|
| ------|---------------|----|
|-m	    |--mode=      	|对比模式，必传。1-同步对比，2-分步对比
|-p	    |--period=	    |分步阶段，该参数仅在分步模式下有效，分步模式下必传。1-读取src数据写入文件，2-读取文件并与dst数据对比
|-f     |--cfg_file=	|配置文件路径，绝对路径或与脚本同路径，必传
|-i	    |--start_idx=	|比对_id文件起始下标，可在配置文件配置，以命令优先
|-c	    |--count=		|比对数量，可在配置文件配置，以命令优先
|-b	    |--batch=		|单次查询批量大小，可在配置文件配置，以命令优先
|-t	    |--task_count=	|并发任务数，可在配置文件配置，以命令优先
    
## 配置文件选项
|key		| Desc | 
|-----------|------|
|compare_dbs	  |   列表，对比的dbs
|compare_colls	  |   列表，对比的collections
|src_url		  |   源数据库url
|dst_url		  |   目标数据库url
|sample_file_name|	 比对_id文件路径，绝对路径或与脚本同路径
|query_batch	  |   单次查询批量大小，与命令-b/--batch相同，以命令传值优先
|sample_start_idx|	 比对_id文件起始下标，与命令-i/--start_idx相同，以命令传值优先
|sample_count	  |   比对数量，与命令-c/--count相同，以命令传值优先
|task_count	      |   并发任务数，与命令-t/--task_count相同，以命令传值优先

## 使用
1.使用python运行，要求环境python3
>
```python comparison.py -m 2 -f compare_conf.json -p 1 -i 0 -c 200 -b 20 -t 3```

```python comparison.py --mode=2 --cfg_file=compare_conf.json --period=1 --start_idx=0 --count=200 --batch=20 --task_count=3```

2.使用exe文件运行，无环境要求
>
```comparison.exe -m 1 -i 0 -c 5000 -b 20```

```comparison.exe --mode=1 --start_idx=0 --count=5000 --batch=20```