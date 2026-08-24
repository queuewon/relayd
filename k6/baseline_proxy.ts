import http from "k6/http";
import { check, sleep } from "k6";
import { Counter } from "k6/metrics";

const backendACounter = new Counter("backend_a_count");
const backendBCounter = new Counter("backend_b_count");

export const options = {
  vus: 10,
  duration: "30s",
  summaryTrendStats: ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)"],
};

export default function () {
  const res = http.get("http://localhost:8080/");
  check(res, { "status is 200": (r) => r.status === 200 });

  if ((res.body as string).includes("A")) {
    backendACounter.add(1);
  } else if ((res.body as string).includes("B")) {
    backendBCounter.add(1);
  }

  sleep(0.1);
}
