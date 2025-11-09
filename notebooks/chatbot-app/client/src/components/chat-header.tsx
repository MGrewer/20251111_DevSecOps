export function ChatHeader() {
  const navigate = useNavigate();
  const { open } = useSidebar();
  const { chatHistoryEnabled } = useConfig();

  const { width: windowWidth } = useWindowSize();

  return (
    <header className="sticky top-0 flex items-center gap-2 bg-background px-2 py-1.5 md:px-2">
      <SidebarToggle />

      <div className="flex-1 flex justify-center">
        <img src="/logo.png" alt="Logo" className="h-8" />
      </div>

      {(!open || windowWidth < 768) && (
        <Button
          variant="outline"
          className="order-2 h-8 px-2 md:h-fit md:px-2"
          onClick={() => {
            navigate('/');
          }}
        >
          <PlusIcon />
          <span className="md:sr-only">New Chat</span>
        </Button>
      )}

      {!chatHistoryEnabled && (
        <TooltipProvider>
          <Tooltip>
            <TooltipTrigger asChild>
              <div className="flex items-center gap-1.5 rounded-full bg-muted px-2 py-1 text-muted-foreground text-xs">
                <CloudOffIcon className="h-3 w-3" />
                <span className="hidden sm:inline">Ephemeral</span>
              </div>
            </TooltipTrigger>
            <TooltipContent>
              <p>Chat history disabled - conversations are not saved</p>
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>
      )}
    </header>
  );
}