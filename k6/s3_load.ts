import http from "k6/http";
import { check } from "k6";
import { Trend } from "k6/metrics";

const normalBackendDuration = new Trend("normal_backend_duration", true);

export const options = {
  scenarios: {
    steady: {
      executor: "constant-arrival-rate",
      rate: 200,
      timeUnit: "1s",
      duration: "40s",
      preAllocatedVUs: 500,
      maxVUs: 800,
    },
  },
  summaryTrendStats: ["avg", "min", "med", "p(95)", "p(99)", "max"],
};

export default function () {
  const res = http.get("http://127.0.0.1:8080/");

  check(res, { "status is 200": (r) => r.status === 200 });

  if (
    res.status === 200 &&
    typeof res.body === "string" &&
    res.body.includes("hello from")
  ) {
    normalBackendDuration.add(res.timings.duration);
  }
}
