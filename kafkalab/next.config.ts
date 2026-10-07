import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // The development badge otherwise intercepts the first chart tab on small screens.
  devIndicators: false,
};

export default nextConfig;
