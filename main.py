from apscheduler.schedulers.blocking import BlockingScheduler
from scripts.test_insert_simple_snapshot import generate_snapshot

scheduler = BlockingScheduler()

@scheduler.scheduled_job('interval', seconds=10)  # ⏱ 每 10 秒执行一次
def job():
    print("🔄 正在生成 QC 快照...")
    generate_snapshot()

if __name__ == '__main__':
    print("✅ QC Snapshot Service 启动中，每 10 秒执行一次快照聚合")
    scheduler.start()