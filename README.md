# 简易存储引擎

[![codecov](https://codecov.io/gh/InsZVA/saver/branch/master/graph/badge.svg)](https://codecov.io/gh/InsZVA/saver)
[![build](https://travis-ci.org/InsZVA/saver.svg?branch=master)](https://travis-ci.org/InsZVA/saver)

LSM树解决方案
使用2-Chunk作为日志存储
SkipList作为内存表
SSTable作为文件层

Chunk结构:
```
+--------+------+---------+ block内的chunk:chunkFull
|checkSum|length|chunkType| 跨block的chunk:chunkFirst+[chunkMid]+chunkLast
+--------+------+---------+
```

SkipList结构:
```
L3 ------------------->kv->
                       |
                       v
L2 --------->-kv------>kv->
              |        |
              v        v
L1 -kv->-kv->-kv->-kv->kv->
```

SSTable结构:
```
+---------+
|kv...kv..|   按键增顺序的值
+---------+
|kv...kv..|
+---------+
|kidx.....|   MetaData使用递增键，
+---------+
|..keyidx.|   方便二分查找
+---------+
```