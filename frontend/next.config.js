/** @type {import('next').NextConfig} */

const backend = process.env.BACKEND_URL ?? "http://localhost:4000"
const generator = process.env.GENERATOR_URL ?? "http://localhost:4001"
const zipkin = process.env.ZIPKIN_URL ?? "http://localhost:9411"

const nextConfig = {
    // Increase timeout for long-running operations like bulk load
    experimental: {
        proxyTimeout: 300000, // 5 minutes (in milliseconds)
    },
    async rewrites() {
        return [
            {
                source: '/api/:path*',
                destination: `${backend}/:path*`,
            },
            {
                source: '/generate/:path*',
                destination: `${generator}/generate/:path*`,
            },
            {
                source: '/zipkin/:path*',
                destination: `${zipkin}/zipkin/:path*`,
            }
        ]
    },
}

module.exports = nextConfig 