export interface FeedMeta {
  feed_type: string
  user_count: number
}

export interface FeedsResponse {
  feeds: Record<string, FeedMeta>
}

export interface FeedMember {
  did: string
  handle?: string
  displayName?: string
  createdAt: string
}

export interface FeedMembersResponse {
  members: FeedMember[]
  page: number
  totalPages: number
  totalCount: number
}

export interface AddUserRequest {
  feedName: string
  handle: string
}

export interface RemoveUserRequest {
  feedName: string
  handle: string
}
