
import sys
sys.path.append('./query_processor')
sys.path.append('./query_optimizer')
sys.path.append('./storage_manager')
sys.path.append('./concurrency_control_manager')
sys.path.append('./failure_recovery_manager')

# NOTE: karena ada folder yang sama di beberapa repo (kaya folder model di setiap repo kan ada, nanti bakal clash dan mungkin error)
# fixnya pas import pake nama foldernya, contoh: asalnya `from model.blabla import fungsi` jadi `from query_processor.model.blabla import fungsi`

from QueryProcessor import QueryProcessor

if __name__ == "__main__":
    q = QueryProcessor()
    print(q.execute_query("SELECT FROM BLABLABLA"))
