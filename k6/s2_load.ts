import http from "k6/http";
import { check } from "k6";

export const options = {
  scenarios: {
    steady: {
      executor: "constant-arrival-rate",
      rate: 200, // 초당 200요청
      timeUnit: "1s",
      duration: "40s",
      preAllocatedVUs: 50,
      maxVUs: 200,
    },
  },
};

export default function () {
  const res = http.get("http://127.0.0.1:8080/");
  check(res, { "status is 200": (r) => r.status === 200 });
}
