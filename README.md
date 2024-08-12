# splitFast

分割文件

可以设定分割大小，精确分割文件，也可以设置分割符，按照分割符尽量接近分割大小。异步分割，适合大文件分割

速度比linux自带的split快的多，特别是对比大文件

可以调节并发读取数

--help

```
Usage:
  splitFast [flags]

Flags:
  -b, --bytes=SIZE int            分割后的子文件大小, 单位为GB, 注意: 如果splitElment为none, 会精确分割的子文件大小; 如果有分割符(return, space), 文件会以分割符分割，文件大小尽量为接近此大小 (default 20)
  -c, --check                     是否检查模式, 检测分割后的文件与原文件是否一致, 默认为false, 即分割模式
  -f, --filePath string           需要分割的对象文件路径, 必填
  -h, --help                      help for splitFast
  -o, --output string             分割后的文件目录，默认与分割对象文件相同
  -p, --pool int                  最大并发数 (default 5)
  -e, --splitElment string        分割时会根据splitElment来分割，可选(none, return, space)，当为none时，以精确的bytesSize分割，当为return时，以近似bytesSize并且寻找最近换行符分割，当为space时以近似bytesSize并且寻找最近空格分割 (default "return")
  -s, --splited filename string   分割后的子文件名, 其中{%d}为子文件序号，{%s}为分割对象文件名 (default "part{%d}.{%s}")
```

