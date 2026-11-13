"""Manda kills aleatórias pra API, pra ver o ranking mexendo em http://127.0.0.1:8000/

    python scripts/simulate.py --rate 3 --seconds 60
"""

import argparse
import random
import time

import httpx

PLAYERS = [("p1", "Lobo"), ("p2", "Corvo"), ("p3", "Raposa"), ("p4", "Tatu"), ("p5", "Coruja"),
           ("p6", "Onça"), ("p7", "Gavião"), ("p8", "Jacaré"), ("p9", "Lince"), ("p10", "Puma")]
WEAPONS = [("M4-A1", 40, 300), ("AKM", 30, 250), ("Mosin9130", 100, 800), ("SVD", 150, 900),
           ("MP5K", 5, 60), ("Shotgun", 2, 25)]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--key", default="dev-key")
    parser.add_argument("--rate", type=float, default=2, help="kills por segundo")
    parser.add_argument("--seconds", type=float, default=30)
    args = parser.parse_args()

    # alguns jogadores são melhores que outros, senão o ranking fica empatado
    weights = [10 - i for i in range(len(PLAYERS))]
    end = time.monotonic() + args.seconds
    with httpx.Client(base_url=args.url, headers={"X-Api-Key": args.key}) as client:
        while time.monotonic() < end:
            killer, victim = random.choices(PLAYERS, weights=weights, k=1)[0], random.choice(PLAYERS)
            if killer == victim:
                continue
            weapon, lo, hi = random.choice(WEAPONS)
            body = {"killer_id": killer[0], "killer_name": killer[1], "victim_id": victim[0],
                    "victim_name": victim[1], "weapon": weapon,
                    "distance": round(random.uniform(lo, hi), 1), "headshot": random.random() < 0.25}
            r = client.post("/events/kill", json=body)
            print(r.status_code, killer[1], "→", victim[1], weapon, body["distance"], "m")
            time.sleep(1 / args.rate)


if __name__ == "__main__":
    main()
