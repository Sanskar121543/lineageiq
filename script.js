import http from 'k6/http';
import { check } from 'k6';

export const options = {
  vus: 5,
  duration: '10s',
};

export default function () {
  const payload = JSON.stringify({
    dataset: "orders",
    column: "customer_id",
    change_type: "rename_column"
  });

  const res = http.post(
    'http://host.docker.internal:8000/api/v1/impact/blast-radius',
    payload,
    { headers: { 'Content-Type': 'application/json' } }
  );

  check(res, {
    'status 200': (r) => r.status === 200,
  });
}