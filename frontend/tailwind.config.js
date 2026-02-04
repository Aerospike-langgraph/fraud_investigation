/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    './pages/**/*.{js,ts,jsx,tsx,mdx}',
    './components/**/*.{js,ts,jsx,tsx,mdx}',
    './app/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  darkMode: ['selector', '[data-theme="dark"]', 'class'],
  theme: {
  	extend: {
  		colors: {
  			border: 'hsl(var(--border))',
  			input: 'hsl(var(--input))',
  			ring: 'hsl(var(--ring))',
  			background: 'hsl(var(--background))',
  			foreground: 'hsl(var(--foreground))',
  			primary: {
  				'50': '#E1F4FB',
  				'100': '#B3E0F2',
  				'200': '#81D1EF',
  				'300': '#61BFE4',
  				'400': '#2FB2E5',
  				'500': '#08A5E1',
  				'600': '#0097D3',
  				'700': '#0085C0',
  				'800': '#0074AC',
  				'900': '#00548B',
  				DEFAULT: 'hsl(var(--primary))',
  				foreground: 'hsl(var(--primary-foreground))'
  			},
  			secondary: {
  				DEFAULT: 'hsl(var(--secondary))',
  				foreground: 'hsl(var(--secondary-foreground))'
  			},
  			muted: {
  				DEFAULT: 'hsl(var(--muted))',
  				foreground: 'hsl(var(--muted-foreground))'
  			},
  			accent: {
  				DEFAULT: 'hsl(var(--accent))',
  				foreground: 'hsl(var(--accent-foreground))'
  			},
  			destructive: {
  				DEFAULT: 'hsl(var(--destructive))',
  				foreground: 'hsl(var(--destructive-foreground))'
  			},
  			success: {
  				'50': '#E8F6ED',
  				'100': '#C7E9D3',
  				'200': '#A4DBB8',
  				'300': '#7ECD9C',
  				'400': '#61C287',
  				'500': '#44B772',
  				'600': '#3DA867',
  				'700': '#34955B',
  				'800': '#2D8450',
  				'900': '#22643C'
  			},
  			warning: {
  				'50': '#FEF4EC',
  				'100': '#FDE9D8',
  				'200': '#FBD4B1',
  				'300': '#F9BE8B',
  				'400': '#F7A964',
  				'500': '#F69E51',
  				'600': '#F5933D',
  				'700': '#F4882A',
  				'800': '#F07D1F',
  				'900': '#D2660F'
  			},
  			danger: {
  				'50': '#FFEFED',
  				'100': '#FFD6D1',
  				'200': '#FAC3BB',
  				'300': '#F6988B',
  				'400': '#F26552',
  				'500': '#F55937',
  				'600': '#E85137',
  				'700': '#D64731',
  				'800': '#C9412B',
  				'900': '#BA371F'
  			},
  			card: {
  				DEFAULT: 'hsl(var(--card))',
  				foreground: 'hsl(var(--card-foreground))'
  			},
  			popover: {
  				DEFAULT: 'hsl(var(--popover))',
  				foreground: 'hsl(var(--popover-foreground))'
  			},
  			chart: {
  				'1': 'hsl(var(--chart-1))',
  				'2': 'hsl(var(--chart-2))',
  				'3': 'hsl(var(--chart-3))',
  				'4': 'hsl(var(--chart-4))',
  				'5': 'hsl(var(--chart-5))'
  			}
  		},
  		borderRadius: {
  			lg: 'var(--radius)',
  			md: 'calc(var(--radius) - 2px)',
  			sm: 'calc(var(--radius) - 4px)'
  		}
  	}
  },
  plugins: [require("tailwindcss-animate")],
} 