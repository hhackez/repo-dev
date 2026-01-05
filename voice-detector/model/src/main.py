import argparse
from contextlib import closing
from detect import VoiceDetectionModel


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", "-c", help="config yaml file path", required=True)
    parser.add_argument("--task", "-t", help="task_name", required=True)
    parser.add_argument("--yyyymmdd", "-y", help="yyyymmdd", required=True)

    # parse
    args = parser.parse_args()

    print("============================")
    print(f">> task: {args.task}")
    print(f">> config: {args.config}")
    print(f">> yyyymmdd: {args.yyyymmdd}")
    print("============================")

    return args


def main():
    args = parse_args()

    # args = parser.parse_args()

    print("============================")
    print(f">> task: {args.task}")
    print(f">> config: {args.config}")
    print(f">> yyyymmdd: {args.yyyymmdd}")
    print("============================")

    task_mapping = {
        # "create": CreateSource,
        "detect": VoiceDetectionModel,
    }

    task = task_mapping[args.task]

    with closing(
        task(task=args.task, config_path=args.config, yyyymmdd=args.yyyymmdd)
    ) as app:
        app.run()


if __name__ == "__main__":
    main()
