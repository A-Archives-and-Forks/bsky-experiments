import { useState, useEffect } from 'react'
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import { FeedList } from './components/FeedList'
import { FeedDetail } from './components/FeedDetail'
import { ApiKeyPrompt } from './components/ApiKeyPrompt'
import { getApiKey } from './lib/api'

function App() {
  const [hasApiKey, setHasApiKey] = useState(false)

  useEffect(() => {
    const key = getApiKey()
    setHasApiKey(!!key)
  }, [])

  const handleApiKeySet = () => {
    setHasApiKey(true)
  }

  if (!hasApiKey) {
    return <ApiKeyPrompt onApiKeySet={handleApiKeySet} />
  }

  return (
    <BrowserRouter basename="/dashboard">
      <div className="min-h-screen bg-background">
        <header className="border-b">
          <div className="container mx-auto px-4 py-4">
            <h1 className="text-2xl font-bold">Feedgen Dashboard</h1>
          </div>
        </header>
        <main className="container mx-auto px-4 py-8">
          <Routes>
            <Route path="/" element={<FeedList />} />
            <Route path="/feed/:feedName" element={<FeedDetail />} />
            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  )
}

export default App
