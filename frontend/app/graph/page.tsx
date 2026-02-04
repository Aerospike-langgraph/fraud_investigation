'use client'

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Network, Users, CreditCard, AlertTriangle } from 'lucide-react'

export default function GraphPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold tracking-tight">Graph Visualization</h1>
        <p className="text-muted-foreground">
          Interactive graph view of user and transaction networks (Phase 2)
        </p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Network className="h-5 w-5" />
            Graph View Coming Soon
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-center py-12">
            <Network className="h-16 w-16 mx-auto text-muted-foreground mb-4" />
            <h3 className="text-lg font-semibold mb-2">Interactive Graph Visualization</h3>
            <p className="text-muted-foreground mb-6 max-w-md mx-auto">
              This feature will provide an interactive graph view showing the relationships 
              between users, accounts, and transactions. You'll be able to explore fraud 
              patterns visually and identify suspicious connections.
            </p>
            
            <div className="grid gap-4 md:grid-cols-3 max-w-2xl mx-auto">
              <div className="flex items-center gap-2 p-3 border rounded-lg">
                <Users className="h-5 w-5 text-primary" />
                <span className="text-sm">User Nodes</span>
              </div>
              <div className="flex items-center gap-2 p-3 border rounded-lg">
                <CreditCard className="h-5 w-5 text-primary" />
                <span className="text-sm">Transaction Edges</span>
              </div>
              <div className="flex items-center gap-2 p-3 border rounded-lg">
                <AlertTriangle className="h-5 w-5 text-destructive" />
                <span className="text-sm">Fraud Indicators</span>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      <div className="grid gap-4 md:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Planned Features</CardTitle>
          </CardHeader>
          <CardContent>
            <ul className="space-y-2 text-sm">
              <li className="flex items-center gap-2">
                <div className="w-2 h-2 bg-primary rounded-full"></div>
                Interactive node dragging and zooming
              </li>
              <li className="flex items-center gap-2">
                <div className="w-2 h-2 bg-primary rounded-full"></div>
                Color-coded risk levels
              </li>
              <li className="flex items-center gap-2">
                <div className="w-2 h-2 bg-primary rounded-full"></div>
                Fraud pattern highlighting
              </li>
              <li className="flex items-center gap-2">
                <div className="w-2 h-2 bg-primary rounded-full"></div>
                Real-time graph updates
              </li>
              <li className="flex items-center gap-2">
                <div className="w-2 h-2 bg-primary rounded-full"></div>
                Export graph data
              </li>
            </ul>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Technology Stack</CardTitle>
          </CardHeader>
          <CardContent>
            <ul className="space-y-2 text-sm">
              <li className="flex items-center gap-2">
                <div className="w-2 h-2 bg-success rounded-full"></div>
                D3.js for graph rendering
              </li>
              <li className="flex items-center gap-2">
                <div className="w-2 h-2 bg-success rounded-full"></div>
                Force-directed layouts
              </li>
              <li className="flex items-center gap-2">
                <div className="w-2 h-2 bg-success rounded-full"></div>
                WebGL for performance
              </li>
              <li className="flex items-center gap-2">
                <div className="w-2 h-2 bg-success rounded-full"></div>
                Real-time WebSocket updates
              </li>
              <li className="flex items-center gap-2">
                <div className="w-2 h-2 bg-success rounded-full"></div>
                Responsive design
              </li>
            </ul>
          </CardContent>
        </Card>
      </div>
    </div>
  )
} 