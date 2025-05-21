import random
import pandas as pd
from datetime import datetime, timedelta
from pm4py.objects.log.obj import EventLog, Trace, Event
from pm4py.objects.log.exporter.xes import exporter as xes_exporter

# === Zufallsseed für Reproduzierbarkeit ===
SEED = 42
random.seed(SEED)

# === Globale Parameter ===
NUM_TRACES = 10000
TIME_BETWEEN_TRACES = 60
today = datetime.today()
BASE_START = datetime(today.year, today.month, today.day, 8, 0, 0)

# === Nachhaltigkeitswertbereiche ===
MATERIAL_INPUT_RANGE = (0.495, 0.505)
ENERGY_KWH_SCHMELZEN_RANGE = (0.75, 0.80)
ENERGY_KWH_SONSTIG_RANGE = (0.117, 0.195)
WATER_L_RANGE = (0.9, 1.1)

# === Ressourcen & Bearbeitungsintervalle pro Prozessschritt ===
RESOURCES = {
    "Materialprüfung": "Sortieranlage_1",
    "Schmelzen": "Schmelzofen_1",
    "Formen": "Blasmaschine_1",
    "Abkühlen": "Kühlofen_1",
    "Qualitätskontrolle": "Kontrollstation_1",
    "Nachbearbeitung": "Nachbehandlungsmodul_1",
    "Waschen": "Spülanlage_1"
}

DURATIONS = {
    "Materialprüfung": (60, 120),
    "Schmelzen": (480, 720),
    "Formen": (120, 240),
    "Abkühlen": (480, 720),
    "Qualitätskontrolle": (30, 120),
    "Nachbearbeitung": (180, 300),
    "Waschen": (90, 180)
}

# === Entscheidungsfunktionen für Prozessverzweigungen ===
def entscheiden_materialprüfung():
    return random.choices(["OK", "Ausschuss"], weights=[0.95, 0.05])[0]

def entscheiden_formen():
    return random.choices(["OK", "Recycling"], weights=[0.95, 0.05])[0]

def entscheiden_qualitätskontrolle():
    return random.choices(["OK", "Rework", "Recycling"], weights=[0.85, 0.10, 0.05])[0]

# === Klasse zur Erzeugung eines Traces ===
class FlaschenTrace:
    def __init__(self, trace_id, start_time):
        self.trace_id = trace_id
        self.time = start_time
        self.events = []
        self.qk_counter = 0  # Qualitätskontrolle-Zähler
        self.nb_counter = 0  # Nachbearbeitungs-Zähler

    def advance_time(self, step_name):
        duration = random.randint(*DURATIONS[step_name])
        self.time += timedelta(seconds=duration)

    def add_event(self, name, status=None, attributes=None):
        base_name = name.split("_")[0]
        event = {
            "case_id": self.trace_id,
            "concept:name": name,
            "time:timestamp": self.time,
            "org:resource": RESOURCES.get(base_name, "Unbekannt")
        }
        if status:
            event["status"] = status
        if attributes:
            event.update(attributes)
        self.events.append(event)

    def generate(self):
        # Materialprüfung
        self.advance_time("Materialprüfung")
        material_input = round(random.uniform(*MATERIAL_INPUT_RANGE), 3)
        energy = round(random.uniform(*ENERGY_KWH_SONSTIG_RANGE), 3)
        status = entscheiden_materialprüfung()

        material_attributes = {
            "sustainability:material_input_kg": material_input,
            "sustainability:energy_kwh": energy
        }

        if status == "Ausschuss":
            material_attributes["sustainability:waste_kg"] = material_input
            self.add_event("Materialprüfung", status=status, attributes=material_attributes)
            return self.events

        self.add_event("Materialprüfung", status=status, attributes=material_attributes)

        # Weitere Schritte
        for step in ["Schmelzen", "Formen", "Abkühlen", "Qualitätskontrolle"]:
            self.advance_time(step)
            if step == "Schmelzen":
                energy = round(random.uniform(*ENERGY_KWH_SCHMELZEN_RANGE), 3)
            else:
                energy = round(random.uniform(*ENERGY_KWH_SONSTIG_RANGE), 3)

            attributes = {"sustainability:energy_kwh": energy}

            if step == "Formen":
                status = entscheiden_formen()
                if status == "Recycling":
                    attributes["sustainability:recycling_kg"] = material_input
                    self.add_event(step, status=status, attributes=attributes)
                    return self.events
                else:
                    self.add_event(step, status=status, attributes=attributes)

            elif step == "Qualitätskontrolle":
                while True:
                    self.qk_counter += 1
                    qk_step_name = f"{step}_{self.qk_counter}"
                    status = entscheiden_qualitätskontrolle()

                    if status == "Recycling":
                        attributes["sustainability:recycling_kg"] = material_input
                        self.add_event(qk_step_name, status=status, attributes=attributes)
                        return self.events

                    elif status == "Rework":
                        self.add_event(qk_step_name, status=status, attributes=attributes)
                        self.advance_time("Nachbearbeitung")
                        self.nb_counter += 1
                        nb_step_name = f"Nachbearbeitung_{self.nb_counter}"
                        self.add_event(nb_step_name, attributes={
                            "sustainability:energy_kwh": round(random.uniform(*ENERGY_KWH_SONSTIG_RANGE), 3)
                        })
                        self.advance_time("Qualitätskontrolle")
                    else:
                        self.add_event(qk_step_name, status=status, attributes=attributes)
                        break

            else:
                self.add_event(step, attributes=attributes)

        # Waschen
        self.advance_time("Waschen")
        self.add_event("Waschen", attributes={
            "sustainability:energy_kwh": round(random.uniform(*ENERGY_KWH_SONSTIG_RANGE), 3),
            "sustainability:water_l": round(random.uniform(*WATER_L_RANGE), 3)
        })

        return self.events

# === Event-Log erzeugen ===
event_log = []
for i in range(NUM_TRACES):
    trace_start = BASE_START + timedelta(seconds=i * TIME_BETWEEN_TRACES)
    trace = FlaschenTrace(f"Flasche_{i+1:04d}", trace_start)
    event_log.extend(trace.generate())

# === Event-Log als CSV speichern ===
df = pd.DataFrame(event_log)
df.to_csv("synthetic_event_log_seed_84.csv", index=False)

# === Event-Log als XES-Datei speichern ===
log = EventLog()

for case_id in df["case_id"].unique():
    trace_df = df[df["case_id"] == case_id].sort_values(by="time:timestamp")
    trace = Trace()
    trace.attributes["concept:name"] = case_id

    for _, row in trace_df.iterrows():
        event = Event()
        for col in df.columns:
            if pd.notna(row[col]):
                if col.startswith("sustainability:"):
                    key = col.replace(":", "_")
                else:
                    key = col

                value = row[col]

                if "timestamp" in key:
                    event[key] = pd.to_datetime(value)
                elif isinstance(value, (int, float)) and not isinstance(value, bool):
                    event[key] = float(value)
                else:
                    event[key] = str(value)
        trace.append(event)
    log.append(trace)

# XES-Datei exportieren
xes_path = "synthetic_event_log_seed_84.xes"
xes_exporter.apply(log, xes_path)

# === XES Header korrigieren für Signavio ===
with open(xes_path, "r", encoding="utf-8") as file:
    lines = file.readlines()

lines[0] = '<?xml version="1.0" encoding="UTF-8"?>\n'
lines[1] = '<log xmlns="http://www.xes-standard.org/">\n'

with open(xes_path, "w", encoding="utf-8") as file:
    file.writelines(lines)
