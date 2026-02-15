import { useState } from 'react'
import { useRemoveUser } from '../hooks/useFeedMembers'
import { Button } from './ui/button'
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from './ui/alert-dialog'
import { Trash2, Loader2 } from 'lucide-react'

interface RemoveUserDialogProps {
  feedName: string
  userDid: string
}

export function RemoveUserDialog({ feedName, userDid }: RemoveUserDialogProps) {
  const [open, setOpen] = useState(false)
  const removeUser = useRemoveUser()

  const handleRemove = async () => {
    try {
      // The API expects a handle, but we have a DID
      // For now, we'll pass the DID as the handle parameter
      // The backend will need to handle this or we need to resolve DID to handle
      await removeUser.mutateAsync({ feedName, handle: userDid })
      setOpen(false)
    } catch (err) {
      console.error('Failed to remove user:', err)
    }
  }

  return (
    <AlertDialog open={open} onOpenChange={setOpen}>
      <AlertDialogTrigger asChild>
        <Button variant="ghost" size="sm" className="text-destructive hover:text-destructive">
          <Trash2 className="h-4 w-4" />
        </Button>
      </AlertDialogTrigger>
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>Remove User from Feed</AlertDialogTitle>
          <AlertDialogDescription>
            Are you sure you want to remove this user from{' '}
            <span className="font-medium">{feedName}</span>?
            <br />
            <span className="font-mono text-xs mt-2 block truncate">{userDid}</span>
          </AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel disabled={removeUser.isPending}>
            Cancel
          </AlertDialogCancel>
          <AlertDialogAction
            onClick={handleRemove}
            disabled={removeUser.isPending}
            className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
          >
            {removeUser.isPending ? (
              <>
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                Removing...
              </>
            ) : (
              'Remove'
            )}
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  )
}
