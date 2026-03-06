import { useState, useEffect, FormEvent } from 'react'
import Dashboard from './Dashboard'
import './App.css'

const STORAGE_KEY = 'api_token'

interface Item {
  id: number
  type: string
  title: string
  description?: string
  created_at: string
}

type Page = 'items' | 'dashboard'

function App() {
  const [token, setToken] = useState(
    () => localStorage.getItem(STORAGE_KEY) ?? '',
  )
  const [draft, setDraft] = useState('')
  const [items, setItems] = useState<Item[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [currentPage, setCurrentPage] = useState<Page>('items')

  useEffect(() => {
    if (!token) return

    setLoading(true)
    setError(null)

    const apiUrl = import.meta.env.VITE_API_URL
    if (!apiUrl) {
      setError('VITE_API_URL is not defined in .env file')
      setLoading(false)
      return
    }

    fetch(`${apiUrl}/items/`, {
      headers: { Authorization: `Bearer ${token}` },
    })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        return res.json()
      })
      .then((data: Item[]) => {
        setItems(data)
        setLoading(false)
      })
      .catch((err: Error) => {
        setError(err.message)
        setLoading(false)
      })
  }, [token])

  function handleConnect(e: FormEvent) {
    e.preventDefault()
    const trimmed = draft.trim()
    if (!trimmed) return
    localStorage.setItem(STORAGE_KEY, trimmed)
    setToken(trimmed)
  }

  function handleDisconnect() {
    localStorage.removeItem(STORAGE_KEY)
    setToken('')
    setDraft('')
    setItems([])
    setError(null)
  }

  if (!token) {
    return (
      <form className="token-form" onSubmit={handleConnect}>
        <h1>API Token</h1>
        <p>Enter your API token to connect.</p>
        <input
          type="password"
          placeholder="Token"
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
        />
        <button type="submit">Connect</button>
      </form>
    )
  }

  return (
    <div>
      <header className="app-header">
        <div className="nav-buttons">
          <button
            className={currentPage === 'items' ? 'active' : ''}
            onClick={() => setCurrentPage('items')}
          >
            Items
          </button>
          <button
            className={currentPage === 'dashboard' ? 'active' : ''}
            onClick={() => setCurrentPage('dashboard')}
          >
            Dashboard
          </button>
        </div>
        <button className="btn-disconnect" onClick={handleDisconnect}>
          Disconnect
        </button>
      </header>

      {currentPage === 'items' && (
        <>
          {loading && <p>Loading...</p>}
          {error && <p>Error: {error}</p>}

          {!loading && !error && (
            <table>
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Type</th>
                  <th>Title</th>
                  <th>Description</th>
                  <th>Created at</th>
                </tr>
              </thead>
              <tbody>
                {items.map((item) => (
                  <tr key={item.id}>
                    <td>{item.id}</td>
                    <td>{item.type}</td>
                    <td>{item.title}</td>
                    <td>{item.description || '-'}</td>
                    <td>{item.created_at}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </>
      )}

      {currentPage === 'dashboard' && <Dashboard token={token} />}
    </div>
  )
}

export default App
