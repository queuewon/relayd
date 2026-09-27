import http from "k6/http";
import { Counter } from "k6/metrics";

// 프록시
const TARGET = "http://127.0.0.1:8080/";
const RATE = 20; // req/s
const DURATION = "120s";

const status503 = new Counter("status_503");
const reasonBurstStart = new Counter("reason_burst_start");
const reasonBurst = new Counter("reason_burst");
const reasonProb = new Counter("reason_prob");
const reasonOther = new Counter("reason_other");

export const options = {
  scenarios: {
    burst: {
      executor: "constant-arrival-rate",
      rate: RATE,
      timeUnit: "1s",
      duration: DURATION,
      preAllocatedVUs: 50,
      maxVUs: 100,
    },
  },
};

export default function () {
  const res = http.get(TARGET);

  if (res.status === 503) {
    status503.add(1);
    const reason = (res.body as string).split("reason=")[1]?.trim();

    if (reason === "burst_start") reasonBurstStart.add(1);
    else if (reason === "burst") reasonBurst.add(1);
    else if (reason === "prob") reasonProb.add(1);
    else reasonOther.add(1);
  }
}
