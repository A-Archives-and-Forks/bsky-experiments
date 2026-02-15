import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api, getApiKey } from '../lib/api'

export function useFeedMembers(feedName: string, page: number, search = '') {
  return useQuery({
    queryKey: ['feedMembers', feedName, page, search],
    queryFn: () => api.getFeedMembers(feedName, page, 25, search),
    enabled: !!feedName && !!getApiKey(),
  })
}

export function useAddUser() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: api.addUserToFeed,
    onSuccess: (_data, variables) => {
      queryClient.invalidateQueries({ queryKey: ['feedMembers', variables.feedName] })
      queryClient.invalidateQueries({ queryKey: ['feeds'] })
    },
  })
}

export function useRemoveUser() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: api.removeUserFromFeed,
    onSuccess: (_data, variables) => {
      queryClient.invalidateQueries({ queryKey: ['feedMembers', variables.feedName] })
      queryClient.invalidateQueries({ queryKey: ['feeds'] })
    },
  })
}
