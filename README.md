# sysbench_papapa
调用sysbench压力测试MySQL服务，将TPS和QPS结果打印到excel，并出图。

## 注意
sysbench只支持1.0以上的版本。

## sysbench安装
见：https://github.com/akopytov/sysbench

## 使用
python sysbench_papapa.py --host=127.0.0.1:3307 --user=hcloud --password=hcloud
结果将打印在sysbench_result文件夹下，result.xlsx是最终结果文件。
