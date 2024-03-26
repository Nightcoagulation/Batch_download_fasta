import pandas as pd
import time
from bioservices import EUtils
from tqdm import tqdm

# 设置每秒发布的URL请求不超过3个
requests_per_second = 3

# 定义函数\指定邮箱创建EUtils对象\使用EFetch方法获取FASTA\提取FASTA格式的序列\去掉序列前的元信息和换行符
def get_fasta_ncbi(entry_id):
    try:
        s = EUtils(email="liuzw@tib.cas.cn")
        fasta_data = s.EFetch("protein", entry_id, rettype="fasta")
        # 检查是否成功获取了数据
        if fasta_data:
            # 提取FASTA格式的序列，去掉序列前的元信息和换行符，然后解码为 utf-8 格式的字符串
            sequence = fasta_data.split(b'\n', 1)[1].replace(b'\n', b'').decode('utf-8')
            # 添加适当的延迟
            time.sleep(3 / requests_per_second)
            return sequence
        else:
            # 如果未成功获取数据，返回None
            return None
    except Exception as e:
        # 处理可能的异常情况如网络连接
        print(f"Error fetching data for {entry_id}: {e}")
        with open('/data/home/liuzw/NR/error_log.txt', 'a') as error_log:
            error_log.write(f"Error fetching data for {entry_id}: {e}\n")
        return None

# 读取 CSV 文件\创建DataFrame对象以存储读取的数据\将'id'列的数据类型转换为字符串
data = pd.read_csv('/data/home/liuzw/NR/sequence_ids.txt')
df = pd.DataFrame(data)
df['id'] = df['id'].astype(str)

# 打开文件以保存 fasta 格式的输出
output_path = '/data/home/liuzw/NR/newyear/hits_output_processed.fasta'
with open(output_path, 'w') as f:
    # 遍历 DataFrame 中的每一行
    for index, row in tqdm(df.iterrows(), total=len(df)):
        # 查询并获取 fasta 数据
        fasta_sequence = get_fasta_ncbi(row['id'])
        # 如果成功获取到 fasta 数据
        if fasta_sequence:
            # 处理 fasta 数据
            processed_sequence = fasta_sequence.replace("RNA", "")
            # 如果处理后的序列非空
            if processed_sequence.strip():
                # 将处理后的序列写入到文件中
                f.write(f'>{row["id"]}\n{processed_sequence}\n')
                f.flush()  # 立即将缓冲区内容写入文件
        tqdm.write(f"Processed {index+1}/{len(df)} sequences")  # 更新进度条