import uvicorn
from fastapi import FastAPI

from scheduler.app import create_app

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


# 调度子系统挂载在 /scheduler 下，既有接口保持不变。
app.mount("/scheduler", create_app())

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
