'use client'

import { useEffect, useRef, useState } from "react"

const DocsPage = () => {
	const frameRef = useRef<HTMLIFrameElement | null>(null);
	const [height, setHeight] = useState("100vh")

	useEffect(() => {
		if(frameRef.current) {
			setTimeout(() => setHeight(`${frameRef?.current?.contentWindow?.document.documentElement.scrollHeight ?? 0}px`), 500);
		}
	}, []);

  	return (
    	<iframe src="/api/docs" className="iframe" style={{ width: "100%", height }} ref={frameRef} />
  	)
}

export default DocsPage