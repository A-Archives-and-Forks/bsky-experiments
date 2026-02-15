import { useQuery } from '@tanstack/react-query'
import { api, getApiKey } from '../lib/api'

export function useFeeds() {
  return useQuery({
    queryKey: ['feeds'],
    queryFn: api.getFeeds,
    enabled: !!getApiKey(),
  })
}
