import { redirect } from "react-router";

export function loader() {
  return redirect("/trading");
}

export default function Index() {
  return null;
}
