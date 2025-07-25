import pendulum
import uuid
import random
import pandas

def generate_data():
    device = ["desktop", "mobil", "Tablet"]
    weights_device = [0.4, 0.4, 0.2]
    link = {"ИПОТЕКА": 1, "ОСАГО": 2, "ДОМ": 3, "ДМС": 4, "КАСКО": 50}
    weights_link = [0.1, 0.4, 0.1, 0.15, 0.25]
    weights_action = [0.6, 0.3, 0.1]
    data_list = []
    for _ in range(1000):
        session_start = pendulum.now()
        session_end = pendulum.now() + pendulum.duration(milliseconds=random.randint(1000, 1000000))
        name_link = random.choices(list(link.keys()), weights=weights_link)[0] if (session_end - session_start).seconds > 15 else None
        action_type = random.choices(['calculation', 'view', 'add to cart'], weights=weights_action)[0] if name_link is not None else 'view'
        data = {"user_id": int(random.randint(100, 500000) * (pendulum.now().timestamp() * 1000 % 10)),
                "session_id": str(uuid.uuid4()).split('-')[0],
                "traffic_source": random.choice(['organic', 'paid', 'social', 'email', 'referral', 'direct']),
                "session_start": session_start,
                "session_end": session_end,
                "device": random.choices(device, weights=weights_device)[0],
                "name_link": name_link,
                "action_type": action_type,
                "purchase_amount": round(random.uniform(1000 * (link[name_link] / 2), 10000 * link[name_link]), 2) if (action_type == "calculation" and name_link is not None) else None,
                }
        data_list.append(data)
    
    return data_list
