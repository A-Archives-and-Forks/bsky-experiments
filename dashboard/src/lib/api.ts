import type { FeedsResponse, FeedMembersResponse, AddUserRequest, RemoveUserRequest } from '../types/feed'

const API_KEY_STORAGE_KEY = 'feedgen_api_key'

// In production (when served from Go binary), use same origin
// In development, use VITE_API_URL env var or default to localhost:8094
const getBaseUrl = (): string => {
  if (import.meta.env.VITE_API_URL) {
    return import.meta.env.VITE_API_URL
  }
  // In production, use same origin (empty string = relative URL)
  if (import.meta.env.PROD) {
    return ''
  }
  // Default for development
  return 'http://localhost:8094'
}

export const getApiKey = (): string | null => {
  const key = localStorage.getItem(API_KEY_STORAGE_KEY)
  // Return null for empty strings too
  return key && key.trim() ? key.trim() : null
}

export const setApiKey = (key: string): void => {
  localStorage.setItem(API_KEY_STORAGE_KEY, key.trim())
}

export const clearApiKey = (): void => {
  localStorage.removeItem(API_KEY_STORAGE_KEY)
}

class ApiError extends Error {
  status: number

  constructor(message: string, status: number) {
    super(message)
    this.status = status
    this.name = 'ApiError'
  }
}

const fetchWithAuth = async (endpoint: string, options: RequestInit = {}): Promise<Response> => {
  const apiKey = getApiKey()
  if (!apiKey) {
    throw new ApiError('API key not set', 401)
  }

  const baseUrl = getBaseUrl()
  const url = `${baseUrl}${endpoint}`

  const response = await fetch(url, {
    ...options,
    headers: {
      ...options.headers,
      'X-API-Key': apiKey,
      'Content-Type': 'application/json',
    },
  })

  if (response.status === 401) {
    throw new ApiError('Unauthorized - check API key', 401)
  }

  if (!response.ok) {
    const data = await response.json().catch(() => ({ error: 'Unknown error' }))
    throw new ApiError(data.error || 'Request failed', response.status)
  }

  return response
}

export const api = {
  getFeeds: async (): Promise<FeedsResponse> => {
    const response = await fetchWithAuth('/api/admin/feeds')
    return response.json()
  },

  getFeedMembers: async (feedName: string, page = 1, limit = 25, search = ''): Promise<FeedMembersResponse> => {
    const params = new URLSearchParams({ feedName, page: String(page), limit: String(limit) })
    if (search.trim()) {
      params.set('search', search.trim())
    }
    const response = await fetchWithAuth(`/api/admin/feed_members?${params}`)
    return response.json()
  },

  addUserToFeed: async ({ feedName, handle }: AddUserRequest): Promise<void> => {
    const params = new URLSearchParams({ feedName, handle })
    await fetchWithAuth(`/api/admin/feed_members?${params}`, { method: 'PUT' })
  },

  removeUserFromFeed: async ({ feedName, handle }: RemoveUserRequest): Promise<void> => {
    const params = new URLSearchParams({ feedName, handle })
    await fetchWithAuth(`/api/admin/feed_members?${params}`, { method: 'DELETE' })
  },
}

export { ApiError }
