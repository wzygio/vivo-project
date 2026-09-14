# 堡垒机旧 HTML 清理工具

将 `clean_old_html.py` 单独复制到堡垒机即可使用。需要已安装 Python 3.8 或以上，无第三方依赖、不联网、不依赖此项目。没有 Python 时，此版本不能直接运行。

## 筛选范围

- 以运行时的工作目录为根目录，也可以用 `--root` 指定绝对路径；不是默认以脚本所在位置为根目录。
- 默认检查 `根目录/子文件夹/*.html` 和 `根目录/子文件夹/Download/*.html`。例如 `根目录/Download/a.html` 也属于第一种。
- 文件后缀 `.html` 不区分大小写；不包含 `.htm`。
- 修改时间必须严格早于本机当地时间的“上个月 1 日 00:00:00”。例如 2026-09-14 运行时，删除 2026-08-01 零点之前的文件；恰好等于该时刻的文件保留。跨年自动处理。
- 不删除根目录自身的 HTML、目录、其他类型文件或新文件。
- 默认不进入其他更深层目录。只有添加 `--recursive` 才扫描所有子目录。
- 跳过符号链接、Windows junction 和其他 reparse point。请在目标目录结构稳定时运行；本工具不是针对并发恶意替换目录设计的安全隔离器。

## 使用

在待清理根目录打开终端，先预览：

```powershell
python C:\Tools\clean_old_html.py
```

确认显示的 Root、截止日期与范围后，永久删除：

```powershell
python C:\Tools\clean_old_html.py --delete
```

也可以指定目录，先小批处理 100 个：

```powershell
python C:\Tools\clean_old_html.py --root "D:\python_project" --delete --limit 100
```

低性能机器可每 50 个暂停 2 秒：

```powershell
python C:\Tools\clean_old_html.py --root "D:\python_project" --delete --batch-size 50 --pause 2
```

确实需要包含任意深度子目录时，先递归预览：

```powershell
python C:\Tools\clean_old_html.py --root "D:\python_project" --recursive
```

随后保留相同参数并增加 `--delete` 执行。默认仅显示前 20 个匹配路径，以减少终端开销；加 `--verbose` 显示全部。预览仍扫描完整范围并给出统计，除非指定 `--limit`。

## 执行行为

流式扫描，找到符合条件的文件就处理，不先统计整个目录；单线程逐个删除，每成功删除 200 个暂停 1 秒。分批主要为了控制负载、反馈进度，不保证总耗时更短。扫描时每约 5 秒报告已检查项目数，但如果底层文件系统调用本身卡住，无法保证按时刷新。

`--delete` 直接调用文件删除接口，不经过回收站，也不需要再次清空回收站。它不能撤销，且不是安全擦除。不会清理已经在回收站中的文件。

Ctrl+C 可以停止；已删除的文件不会恢复，重新运行会继续处理剩余符合条件的文件。`--limit` 在删除模式限制成功删除数。权限不足、只读或被占用的文件会报告错误并继续，不强制修改权限。退出码：0 正常，1 存在文件操作错误，2 参数错误，130 用户中断。

## 验证

测试只创建和删除临时测试文件：

```powershell
python tools/test_clean_old_html.py
```
