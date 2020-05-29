在lab1中，你最终只需要向外部提供以下API：

master.go:
    Master
    结构体定义

    MakeMaster(files []string, nReduce int) *Master
    创建一个Master对象，立即返回

    (m *Master) Done() bool 
    检查Master是否可以结束，立即返回

worker.go:

    func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string)
    执行map任务和reduce任务，不需要立即返回